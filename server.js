import { 
    makeWASocket, // Diperbaiki: Menggunakan named import untuk menghindari TypeError .default
    useMultiFileAuthState, 
    fetchLatestBaileysVersion,
    delay
} from 'baileys';
import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import pino from 'pino';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(cors());
app.use(express.json());
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

let sock = null;
let isConnected = false;
let currentQr = null;
let cachedGroups = []; // Menyimpan list grup untuk dilempar ke frontend

// Helper untuk format nomor telepon internasional secara presisi
const formatJid = (phone) => {
    let clean = phone.replace(/[^0-9]/g, '');
    if (clean.startsWith('0')) {
        clean = '62' + clean.slice(1); // Standard fallback lokal jika lupa input regional
    }
    return clean.includes('@s.whatsapp.net') ? clean : `${clean}@s.whatsapp.net`;
};

// Fungsi untuk mengambil daftar grup terbaru
async function syncGroups() {
    if (!sock || !isConnected) return;
    try {
        console.log('Mengambil daftar grup dari WhatsApp...');
        const groups = await sock.groupFetchAllParticipating();
        cachedGroups = Object.values(groups).map(g => ({
            id: g.id,
            subject: g.subject
        }));
        // Kirim langsung ke semua client yang terkoneksi di frontend
        io.emit('group_list', cachedGroups);
        io.emit('log', { type: 'success', message: `Berhasil sinkronisasi ${cachedGroups.length} grup.` });
    } catch (err) {
        console.error('Gagal mengambil grup:', err);
        io.emit('log', { type: 'error', message: 'Gagal mengambil daftar grup: ' + err.message });
    }
}

// Inisialisasi Utama Koneksi Baileys Engine
async function initWhatsApp(pairingPhone = null) {
    const { state, saveCreds } = await useMultiFileAuthState('auth_session_pansa');
    const { version } = await fetchLatestBaileysVersion();

    // Diperbaiki: Memanggil langsung makeWASocket tanpa .default
    sock = makeWASocket({
        version,
        logger: pino({ level: 'silent' }),
        auth: state,
        printQRInTerminal: !pairingPhone, // Matikan terminal QR jika memakai metode pairing code
        browser: ['PANSA SYSTEM', 'Chrome', '1.0.0']
    });

    // Request Pairing Code via Frontend jika input nomor di-trigger
    if (pairingPhone && !sock.authState.creds.registered) {
        let cleanPhone = pairingPhone.replace(/[^0-9]/g, '');
        setTimeout(async () => {
            try {
                const code = await sock.requestPairingCode(cleanPhone);
                io.emit('pairing_code', { code });
            } catch (err) {
                io.emit('log', { type: 'error', message: 'Gagal generate pairing code: ' + err.message });
            }
        }, 3000);
    }

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            currentQr = qr;
            io.emit('qr', { qr });
            io.emit('status', { isConnected: false, state: 'QR_READY' });
        }

        if (connection === 'close') {
            currentQr = null;
            isConnected = false;
            cachedGroups = [];
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== 401;
            io.emit('status', { isConnected: false, state: 'DISCONNECTED', reconnecting: shouldReconnect });
            io.emit('group_list', []);
            io.emit('log', { type: 'warn', message: 'Koneksi terputus. Mencoba menghubungkan ulang otomatis...' });
            
            // Auto Reconnect Engine jika bukan karena logout (401)
            if (shouldReconnect) initWhatsApp();
        } else if (connection === 'open') {
            currentQr = null;
            isConnected = true;
            io.emit('status', { isConnected: true, state: 'CONNECTED' });
            io.emit('log', { type: 'success', message: '✅ WhatsApp Berhasil Terhubung!' });
            
            // Tunggu sebentar setelah open, lalu sync grup otomatis
            await delay(2000);
            await syncGroups();
        }
    });

    sock.ev.on('creds.update', saveCreds);
}

// Router Dashboard Render
app.get('/', (req, res) => { res.render('index'); });

// Socket Realtime Gateway Matrix Controller
io.on('connection', (socket) => {
    // Kirim status awal ke client saat baru membuka page / reload
    socket.emit('status', { isConnected, state: isConnected ? 'CONNECTED' : (currentQr ? 'QR_READY' : 'INITIALIZING') });
    if (currentQr) socket.emit('qr', { qr: currentQr });
    if (isConnected && cachedGroups.length > 0) socket.emit('group_list', cachedGroups);

    // Menerima request Pairing dari Frontend
    socket.on('request_pairing', async (data) => {
        socket.emit('log', { type: 'info', message: `Meminta pairing code nomor: ${data.phone}...` });
        initWhatsApp(data.phone);
    });

    // Request manual refresh grup dari frontend
    socket.on('refresh_groups', async () => {
        if(isConnected) {
            await syncGroups();
        } else {
            socket.emit('log', { type: 'error', message: 'Gagal refresh: WhatsApp belum terhubung.' });
        }
    });

    // Core Bulk Stream Processor (Anti-memory Leak & Anti-Ban built-in)
    socket.on('start_bulk', async (data) => {
        if (!isConnected) return socket.emit('log', { type: 'error', message: 'WhatsApp belum terhubung.' });

        const { type, list, targetGroupJid } = data;
        socket.emit('log', { type: 'info', message: `Memulai pemrosesan bulk ${type} untuk ${list.length} item.` });

        for (let i = 0; i < list.length; i++) {
            const item = list[i];
            try {
                if (type === 'contact') {
                    // Simpan / Sinkronisasi Kontak ke WhatsApp Buku Alamat Signalling
                    const jid = formatJid(item.phone);
                    await sock.updateContactSignaling([{ id: jid, name: item.name || `Sync ${item.phone}` }]);
                    socket.emit('item_progress', { 
                        index: i, status: 'success', message: `[KONTAK] Sinkronisasi ${item.phone} (${item.name || 'No Name'})` 
                    });
                } else if (type === 'group') {
                    // Add Member ke Group Terpilih langsung dari JID Dropdown
                    const memberJid = formatJid(item.phone);
                    const response = await sock.groupParticipantsUpdate(targetGroupJid, [memberJid], 'add');
                    let statusInfo = response[0]?.status || '200';

                    if (statusInfo === '200') {
                        socket.emit('item_progress', { index: i, status: 'success', message: `[GRUP] Berhasil menambahkan nomor ${item.phone}` });
                    } else if (statusInfo === '403') {
                        socket.emit('item_progress', { index: i, status: 'warn', message: `[GRUP] Privasi ketat untuk nomor ${item.phone} (membutuhkan link undangan).` });
                    } else {
                        socket.emit('item_progress', { index: i, status: 'error', message: `[GRUP] Gagal menambahkan nomor ${item.phone}. Status Code: ${statusInfo}` });
                    }
                }
                
                // Jeda interval 2.5 detik pelindung dari algoritma ban spam bot WhatsApp
                await delay(2500); 
            } catch (err) {
                socket.emit('item_progress', { index: i, status: 'error', message: `Gagal memproses baris ke-${i+1} (${item.phone}): ${err.message}` });
            }
        }
        socket.emit('bulk_complete', { message: `Selesai memproses antrean total ${list.length} data.` });
    });
});

const PORT = 5000;
server.listen(PORT, () => {
    console.log(`\n======================================================`);
    console.log(`🚀 Server Dashboard Engine Berjalan Aktif!`);
    console.log(`🔗 Buka di Termux Anda: http://localhost:${PORT}`);
    console.log(`======================================================\n`);
    initWhatsApp();
});
