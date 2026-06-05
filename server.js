import makeWASocket, { 
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

const formatJid = (phone) => {
    let clean = phone.replace(/[^0-9]/g, '');
    if (clean.startsWith('0')) {
        clean = '62' + clean.slice(1);
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

async function initWhatsApp(pairingPhone = null) {
    const { state, saveCreds } = await useMultiFileAuthState('auth_session_pansa');
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket.default({
        version,
        logger: pino({ level: 'silent' }),
        auth: state,
        printQRInTerminal: !pairingPhone,
        browser: ['Pansa Suite Engine', 'Chrome', '1.0.0']
    });

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
            if (shouldReconnect) initWhatsApp();
        } else if (connection === 'open') {
            currentQr = null;
            isConnected = true;
            io.emit('status', { isConnected: true, state: 'CONNECTED' });
            io.emit('log', { type: 'success', message: '✅ WhatsApp Berhasil Terhubung!' });
            
            // Tunggu sebentar setelah open, lalu sync grup
            await delay(2000);
            await syncGroups();
        }
    });

    sock.ev.on('creds.update', saveCreds);
}

app.get('/', (req, res) => { res.render('index'); });

io.on('connection', (socket) => {
    socket.emit('status', { isConnected, state: isConnected ? 'CONNECTED' : (currentQr ? 'QR_READY' : 'INITIALIZING') });
    if (currentQr) socket.emit('qr', { qr: currentQr });
    if (isConnected && cachedGroups.length > 0) socket.emit('group_list', cachedGroups);

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

    // Core Bulk Stream Processor yang sudah dimodifikasi
    socket.on('start_bulk', async (data) => {
        if (!isConnected) return socket.emit('log', { type: 'error', message: 'WhatsApp belum terhubung.' });

        const { type, list, targetGroupJid } = data;
        socket.emit('log', { type: 'info', message: `Memulai pemrosesan bulk ${type} untuk ${list.length} item.` });

        for (let i = 0; i < list.length; i++) {
            const item = list[i];
            try {
                if (type === 'contact') {
                    // Simpan Kontak
                    const jid = formatJid(item.phone);
                    await sock.updateContactSignaling([{ id: jid, name: item.name || `Sync ${item.phone}` }]);
                    socket.emit('item_progress', { 
                        index: i, status: 'success', message: `[KONTAK] Sinkronisasi ${item.phone} (${item.name || 'No Name'})` 
                    });
                } else if (type === 'group') {
                    // Add Member ke Group Terpilih
                    const memberJid = formatJid(item.phone);
                    const response = await sock.groupParticipantsUpdate(targetGroupJid, [memberJid], 'add');
                    let statusInfo = response[0]?.status || '200';

                    if (statusInfo === '200') {
                        socket.emit('item_progress', { index: i, status: 'success', message: `[GRUP] Berhasil add ${item.phone}` });
                    } else if (statusInfo === '403') {
                        socket.emit('item_progress', { index: i, status: 'warn', message: `[GRUP] Privasi ketat untuk ${item.phone} (butuh invite link).` });
                    } else {
                        socket.emit('item_progress', { index: i, status: 'error', message: `[GRUP] Gagal add ${item.phone}. Status: ${statusInfo}` });
                    }
                }
                await delay(2500); // Batasan anti-ban pencegah deteksi spam spammer
            } catch (err) {
                socket.emit('item_progress', { index: i, status: 'error', message: `Gagal memproses item ke-${i+1}: ${err.message}` });
            }
        }
        socket.emit('bulk_complete', { message: `Selesai memproses total ${list.length} data.` });
    });
});

const PORT = 5000;
server.listen(PORT, () => {
    console.log(`Server Dashboard Engine berjalan aktif di http://localhost:${PORT}`);
    initWhatsApp();
});
