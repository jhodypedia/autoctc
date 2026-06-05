import { 
    makeWASocket, // Named import untuk versi Baileys v6+
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
import fs from 'fs'; 
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
let cachedGroups = []; 

const SESSION_DIR = 'auth_session_pansa';

// Helper Formatter Nomor Telepon (Mendukung Penuh Semua Negara di Dunia)
const formatJid = (phone) => {
    // 1. Hapus semua karakter non-angka termasuk tanda +, spasi, dan tanda hubung (-)
    let clean = phone.replace(/[^0-9]/g, '');
    
    // 2. Jika nomor diawali angka '0', asumsikan itu format lokal Indonesia dan konversi ke 62
    if (clean.startsWith('0')) {
        clean = '62' + clean.slice(1);
    }
    
    // 3. Kembalikan format JID resmi WhatsApp (@s.whatsapp.net) tanpa merusak kode negara lain (e.g. 52, 1, 44)
    return clean.includes('@s.whatsapp.net') ? clean : `${clean}@s.whatsapp.net`;
};

// Mengambil Data Seluruh Daftar Grup WhatsApp
async function syncGroups() {
    if (!sock || !isConnected) return;
    try {
        console.log('Sedang menyinkronkan daftar grup dari WhatsApp...');
        const groups = await sock.groupFetchAllParticipating();
        cachedGroups = Object.values(groups).map(g => ({
            id: g.id,
            subject: g.subject
        }));
        
        io.emit('group_list', cachedGroups);
        io.emit('log', { type: 'success', message: `Berhasil sinkronisasi ${cachedGroups.length} grup.` });
    } catch (err) {
        console.error('Gagal memetakan grup:', err);
        io.emit('log', { type: 'error', message: 'Gagal mengambil daftar grup: ' + err.message });
    }
}

// Inisialisasi Utama Koneksi Baileys Engine Suite
async function initWhatsApp(pairingPhone = null) {
    const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
        version,
        logger: pino({ level: 'silent' }),
        auth: state,
        printQRInTerminal: !pairingPhone,
        browser: ['PANSA SYSTEM', 'Chrome', '1.0.0']
    });

    // Skema Integrasi Pemasangan Perangkat via Pairing Code
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

    // Pemantau Siklus Koneksi (Connection Lifecycle Monitor)
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
            
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = statusCode !== 401;

            io.emit('status', { isConnected: false, state: 'DISCONNECTED', reconnecting: shouldReconnect });
            io.emit('group_list', []);
            
            if (!shouldReconnect) {
                io.emit('log', { type: 'error', message: 'Sesi kedaluwarsa atau di-logout. Menghapus token lama...' });
                
                if (fs.existsSync(SESSION_DIR)) {
                    try {
                        fs.rmSync(SESSION_DIR, { recursive: true, force: true });
                        console.log(`[System] Folder data sesi '${SESSION_DIR}' berhasil dibersihkan.`);
                    } catch (e) {
                        console.error('Gagal membersihkan folder sesi lama:', e.message);
                    }
                }
                setTimeout(() => initWhatsApp(), 2000);
            } else {
                io.emit('log', { type: 'warn', message: `Koneksi terputus (Code: ${statusCode || 'Unknown'}). Menghubungkan ulang otomatis...` });
                initWhatsApp();
            }
        } else if (connection === 'open') {
            currentQr = null;
            isConnected = true;
            io.emit('status', { isConnected: true, state: 'CONNECTED' });
            io.emit('log', { type: 'success', message: '✅ Server Berhasil Membuka Jalur Gateway WhatsApp!' });
            
            await delay(2000);
            await syncGroups();
        }
    });

    sock.ev.on('creds.update', saveCreds);
}

// HTTP View Endpoint
app.get('/', (req, res) => { res.render('index'); });

// WebSocket Real-time Core Pipelines
io.on('connection', (socket) => {
    socket.emit('status', { isConnected, state: isConnected ? 'CONNECTED' : (currentQr ? 'QR_READY' : 'INITIALIZING') });
    if (currentQr) socket.emit('qr', { qr: currentQr });
    if (isConnected && cachedGroups.length > 0) socket.emit('group_list', cachedGroups);

    socket.on('request_pairing', async (data) => {
        socket.emit('log', { type: 'info', message: `Meminta request pairing code untuk nomor target: ${data.phone}...` });
        initWhatsApp(data.phone);
    });

    socket.on('refresh_groups', async () => {
        if(isConnected) {
            await syncGroups();
        } else {
            socket.emit('log', { type: 'error', message: 'Perintah Ditolak: Hubungkan WhatsApp terlebih dahulu.' });
        }
    });

    // Core Bulk Stream Engine
    socket.on('start_bulk', async (data) => {
        if (!isConnected) return socket.emit('log', { type: 'error', message: 'Aksi digagalkan karena server sedang offline.' });

        const { type, list, targetGroupJid } = data;
        socket.emit('log', { type: 'info', message: `Menjalankan mesin protokol bulk ${type} untuk ${list.length} antrean.` });

        for (let i = 0; i < list.length; i++) {
            const item = list[i];
            try {
                if (type === 'contact') {
                    const jid = formatJid(item.phone);
                    
                    // FIXED BUG: Menggunakan upsertContacts menggantikan updateContactSignaling yang sudah deprecated
                    await sock.upsertContacts([{ 
                        id: jid, 
                        name: item.name || `Sync ${item.phone}`,
                        verifiedName: item.name || `Sync ${item.phone}`
                    }]);

                    socket.emit('item_progress', { 
                        index: i, status: 'success', message: `[KONTAK] Sinkronisasi data sukses untuk nomor ${item.phone} (${item.name || 'No Name'})` 
                    });
                } else if (type === 'group') {
                    const memberJid = formatJid(item.phone);
                    const response = await sock.groupParticipantsUpdate(targetGroupJid, [memberJid], 'add');
                    let statusInfo = response[0]?.status || '200';

                    if (statusInfo === '200') {
                        socket.emit('item_progress', { index: i, status: 'success', message: `[GRUP] Anggota ${item.phone} sukses dimasukkan ke dalam grup.` });
                    } else if (statusInfo === '403') {
                        socket.emit('item_progress', { index: i, status: 'warn', message: `[GRUP] Nomor ${item.phone} membatasi undangan masuk via pengaturan privasi.` });
                    } else {
                        socket.emit('item_progress', { index: i, status: 'error', message: `[GRUP] Gagal memproses nomor ${item.phone}. Status Code: ${statusInfo}` });
                    }
                }
                
                // Jeda 2.5 detik per nomor untuk meminimalkan risiko ban/blokir dari sistem WhatsApp
                await delay(2500); 
            } catch (err) {
                socket.emit('item_progress', { index: i, status: 'error', message: `Gagal mengeksekusi indeks ke-${i+1} (${item.phone}): ${err.message}` });
            }
        }
        socket.emit('bulk_complete', { message: `Pemrosesan database massal selesai. Total data: ${list.length} item.` });
    });
});

const PORT = 5000;
server.listen(PORT, () => {
    console.log(`\n======================================================`);
    console.log(`🚀 PANSA AUTOMATION SYSTEM ENGINES ACTIVE!`);
    console.log(`🔗 Dashboard Matrix Suite URL: http://localhost:${PORT}`);
    console.log(`======================================================\n`);
    initWhatsApp();
});
