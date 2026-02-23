// server.js — Deploy this on Railway.app
// GitHub repo: fox-transfer-proxy

import express from 'express';
import FtpClient from 'ftp';
import fetch from 'node-fetch';
import cors from 'cors';

const app = express();
app.use(cors());
app.use(express.json());

app.get('/health', (req, res) => res.json({ ok: true, time: new Date().toISOString() }));

app.post('/transfer', async (req, res) => {
  // Security check
  const secret = req.headers['x-secret'];
  if (secret !== process.env.TRANSFER_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { supabaseFileUrl, ftpHost, ftpUser, ftpPassword, ftpPort, remotePath } = req.body;

  if (!supabaseFileUrl || !ftpHost || !remotePath) {
    return res.status(400).json({ error: 'Missing: supabaseFileUrl, ftpHost, remotePath' });
  }

  try {
    // 1. Download file from Supabase signed URL
    console.log(`[Transfer] Downloading file from Supabase...`);
    const fileResponse = await fetch(supabaseFileUrl);
    if (!fileResponse.ok) {
      throw new Error(`Supabase download failed: ${fileResponse.status} ${fileResponse.statusText}`);
    }
    const arrayBuffer = await fileResponse.arrayBuffer();
    const fileBuffer = Buffer.from(arrayBuffer);
    console.log(`[Transfer] Downloaded ${fileBuffer.length} bytes`);

    // 2. FTP upload to FoxRenderFarm
    console.log(`[Transfer] FTP connecting to ${ftpHost}:${ftpPort || 21}...`);
    
    await new Promise((resolve, reject) => {
      const ftp = new FtpClient();
      
      const timeout = setTimeout(() => {
        try { ftp.destroy(); } catch(e) {}
        reject(new Error('FTP connection timeout after 120s'));
      }, 120000);

      ftp.on('ready', () => {
        console.log(`[Transfer] FTP connected! Uploading to ${remotePath}`);
        
        // Create directory structure first
        const dir = remotePath.substring(0, remotePath.lastIndexOf('/'));
        ftp.mkdir(dir, true, (_mkdirErr) => {
          // Ignore mkdir errors (dir may already exist)
          ftp.put(fileBuffer, remotePath, (putErr) => {
            clearTimeout(timeout);
            ftp.end();
            if (putErr) {
              reject(new Error(`FTP put failed: ${putErr.message}`));
            } else {
              resolve(true);
            }
          });
        });
      });

      ftp.on('error', (err) => {
        clearTimeout(timeout);
        reject(new Error(`FTP error: ${err.message}`));
      });

      ftp.connect({
        host: ftpHost,
        user: ftpUser || 'anonymous',
        password: ftpPassword || '',
        port: parseInt(String(ftpPort)) || 21,
        connTimeout: 30000,
        pasvTimeout: 30000,
        keepalive: 10000,
      });
    });

    console.log(`[Transfer] ✅ Success: ${remotePath}`);
    res.json({ 
      success: true, 
      remotePath, 
      size: fileBuffer.length,
      message: 'File transferred to FoxRenderFarm successfully'
    });

  } catch (err) {
    console.error('[Transfer] ❌ Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Fox Transfer Proxy running on port ${PORT}`);
});
