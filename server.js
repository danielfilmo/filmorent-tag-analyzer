const express = require('express');
const Anthropic = require('@anthropic-ai/sdk');
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');
const os = require('os');

const app = express();
app.use(express.json());
// Imagenes propias servidas publicamente para poder mandarlas por WhatsApp:
// la URL de un adjunto tiene que ser publica. Aqui vive la de medidas del
// Filmo Grand, que en filmorent.com solo existe en .webp (llega como link).
app.use('/assets', express.static(path.join(__dirname, 'assets'), { maxAge: '7d' }));

// Config
const RESPONDIO_API_KEY = process.env.RESPONDIO_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const GOOGLE_SHEETS_URL = process.env.GOOGLE_SHEETS_URL;
const BACKFILL_TOKEN = process.env.BACKFILL_TOKEN; // v7.4: habilita GET /backfill si esta seteada
const PORT = process.env.PORT || 3000;

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const openai = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;

// Extrae de forma robusta el texto de una respuesta de Claude. OJO: claude-sonnet-5 puede
// devolver content[0] como un bloque NO-texto (ej. razonamiento), dejando content[0].text
// undefined -> `.trim()` truena y la conversacion NO se loguea. Este helper busca el primer
// bloque de tipo 'text'; si no hay, devuelve '' (la fila se loguea igual, sin analisis).
function claudeText(resp) {
  const blocks = (resp && Array.isArray(resp.content)) ? resp.content : [];
  const t = blocks.find(b => b && b.type === 'text' && typeof b.text === 'string');
  return (t ? t.text : '').trim();
}

// v7.1: Transcribe audio from URL using OpenAI Whisper
async function transcribeAudio(audioUrl) {
  if (!openai) {
    console.log('OpenAI not configured, skipping audio transcription');
    return null;
  }

  try {
    // Download audio file to temp
    const response = await fetch(audioUrl);
    if (!response.ok) {
      console.error('Failed to download audio: ' + response.status);
      return null;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const tempFile = path.join(os.tmpdir(), 'voice_' + Date.now() + '.ogg');
    fs.writeFileSync(tempFile, buffer);

    console.log('Audio downloaded: ' + buffer.length + ' bytes, transcribing...');

    // Transcribe with Whisper
    const transcription = await openai.audio.transcriptions.create({
      file: fs.createReadStream(tempFile),
      model: 'whisper-1',
      language: 'es',
      response_format: 'text'
    });

    // Cleanup temp file
    fs.unlinkSync(tempFile);

    const text = transcription.trim();
    console.log('Transcription: "' + text + '"');
    return text;
  } catch (error) {
    console.error('Transcription error: ' + error.message);
    return null;
  }
}

// Agent mapping: userId -> {name, email, type}
const AGENT_MAP = {
  1026911: { name: 'Daniel Alonso', email: 'daniel@filmorent.com', type: 'human' },
  1027747: { name: 'Barush Villarreal', email: 'barush@filmorent.com', type: 'human' },
  1027751: { name: 'Alfredo Celedon', email: 'alfredo@filmorent.com', type: 'human' },
  1027755: { name: 'Eddy Manzano', email: 'eddy@filmorent.com', type: 'human' },
  1027757: { name: 'Diego Tovar', email: 'diego@filmorent.com', type: 'human' },
  1027820: { name: 'Suheidi Dominguez', email: 'administracion@filmorent.com', type: 'human' },
  1028000: { name: 'Filmorent Assistant', email: '', type: 'bot' }
};

// v7: Agent roles for context-aware evaluation
function getAgentRole(name) {
  const roles = {
    'Daniel Alonso': 'owner',
    'Suheidi Dominguez': 'admin',
    'Filmorent Assistant': 'bot',
    'Workflow Automatizado': 'bot'
  };
  return roles[name] || 'sales';
}

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok', version: 'v8.45.0', voz: true, lineaInstantanea: true, ordenes: true, colaAnalisis: true, whisper: !!openai, autoSummary: true, rewards: !!BOOQABLE_API_KEY, staffGoogle: !!REWARDS_GOOGLE_CLIENT_ID, staffProtected: REWARDS_STAFF_PROTECTED, atribuciones: true }));

function extractContactId(body) {
  return (
    body?.contact?.id ||
    body?.data?.contact?.id ||
    body?.contactId ||
    body?.contact_id ||
    body?.data?.contactId ||
    body?.data?.contact_id ||
    body?.conversation?.contactId ||
    body?.data?.conversation?.contactId ||
    null
  );
}

function extractContactName(body) {
  const firstName = body?.contact?.firstName || body?.data?.contact?.firstName || '';
  const lastName = body?.contact?.lastName || body?.data?.contact?.lastName || '';
  const fullName = (firstName + ' ' + lastName).trim();
  return fullName || body?.contact_name || body?.data?.contact_name || 'Desconocido';
}

/**
 * Timestamp (ms) de un mensaje. Respond.io no expone un campo plano: la marca de
 * tiempo vive dentro de status[] (pending/sent/delivered). Tomamos la mas temprana.
 */
function msgTimestamp(msg) {
  let best = null;
  const st = msg && msg.status;
  if (Array.isArray(st)) {
    for (const s of st) {
      const t = s && s.timestamp;
      if (typeof t === 'number' && (best === null || t < best)) best = t;
    }
  }
  return best;
}

/**
 * Extract agents who ACTUALLY SENT messages.
 * Uses sender.source: "user" = human, "ai_agent" = bot
 *
 * BUGFIX 27-jul-2026: la evaluacion se dispara cuando la conversacion se CIERRA, y la
 * fila se sella con la fecha de cierre. Sin ventana, se evaluaba a CUALQUIERA que hubiera
 * escrito alguna vez en esa conversacion, aunque hubiera sido meses antes. Verificado:
 * a Barush lo calificaron estando de vacaciones (21-27 jul) por mensajes del 19-mar al
 * 14-jul. Tambien explica que TODOS bajaran a la vez y que "cierre" se desplomara parejo:
 * al cerrarse un lote de conversaciones viejas, nadie tiene un cierre decente en ellas.
 *
 * @param {number|null} sinceMs  Si se pasa, solo cuenta agentes con mensajes salientes
 *                               DENTRO de la ventana (>= sinceMs). Los mensajes sin
 *                               timestamp legible NO se descartan (no castigar por falta
 *                               de dato), pero tampoco marcan actividad reciente por si solos.
 */
function extractAgentsFromMessages(messages, sinceMs) {
  const humanAgents = new Map();
  const botAgents = new Map();
  const lastActivity = {};   // userId -> ms del ultimo mensaje

  for (const msg of messages) {
    const traffic = msg.traffic || msg.type;
    if (traffic !== 'outgoing') continue;

    const sender = msg.sender;
    if (!sender) continue;

    const uid = sender.userId;
    const source = sender.source;

    const ts = msgTimestamp(msg);
    if (uid != null && ts && (!lastActivity[uid] || ts > lastActivity[uid])) lastActivity[uid] = ts;
    // Fuera de la ventana -> ese mensaje no acredita participacion.
    if (sinceMs && ts && ts < sinceMs) continue;

    if (source === 'ai_agent') {
      if (!botAgents.has(uid)) {
        const known = AGENT_MAP[uid];
        botAgents.set(uid, {
          userId: uid,
          name: known ? known.name : 'Agente Virtual #' + uid,
          email: '',
          type: 'bot'
        });
        if (!AGENT_MAP[uid]) {
          AGENT_MAP[uid] = { name: 'Agente Virtual #' + uid, email: '', type: 'bot' };
          console.log('New bot discovered: ID ' + uid);
        }
      }
    } else if (source === 'user' && uid) {
      if (!humanAgents.has(uid)) {
        const known = AGENT_MAP[uid];
        humanAgents.set(uid, {
          userId: uid,
          name: known ? known.name : 'Agente #' + uid,
          email: known ? known.email : '',
          type: 'human'
        });
        if (!AGENT_MAP[uid]) {
          AGENT_MAP[uid] = { name: 'Agente #' + uid, email: '', type: 'human' };
          console.log('New agent discovered: ID ' + uid);
        }
      }
    } else if (source === 'workflow') {
      if (!botAgents.has('workflow')) {
        botAgents.set('workflow', {
          userId: 'workflow',
          name: 'Workflow Automatizado',
          email: '',
          type: 'bot'
        });
      }
    }
  }

  const stamp = a => Object.assign({}, a, {
    ultimaActividad: lastActivity[a.userId] || null
  });

  return {
    humans: Array.from(humanAgents.values()).map(stamp),
    bots: Array.from(botAgents.values()).map(stamp),
    all: [...Array.from(botAgents.values()), ...Array.from(humanAgents.values())].map(stamp)
  };
}

async function logToGoogleSheets(data) {
  if (!GOOGLE_SHEETS_URL) {
    console.log('Google Sheets URL not configured, skipping log.');
    return;
  }
  try {
    const response = await fetch(GOOGLE_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
      redirect: 'follow'
    });
    if (response.ok) {
      console.log('Logged to Google Sheets successfully.');
    } else {
      console.error('Google Sheets log failed: ' + response.status);
    }
  } catch (error) {
    console.error('Google Sheets log error: ' + error.message);
  }
}

// ============================================================
// v8.37: Cola de analisis para la Mac mini (migracion del Tag
// Analyzer a local, aprobada por Daniel 25-ago-2026). El webhook
// ENCOLA aqui ademas de analizar; la Mac consume via
// GET /cola_analisis?key=... y confirma via POST /cola_analisis/done.
// En fase sombra el server SIGUE analizando normal; al cortar,
// se apaga el analisis local del server y queda solo la cola.
// Persistencia en disco (sobrevive restarts; un deploy la limpia).
// ============================================================
const COLA_ANALISIS_FILE = path.join(__dirname, 'cola_analisis.json');
let colaAnalisis = [];
try { colaAnalisis = JSON.parse(fs.readFileSync(COLA_ANALISIS_FILE, 'utf8')); } catch (e) {}
function colaAnalisisSave() {
  try { fs.writeFileSync(COLA_ANALISIS_FILE, JSON.stringify(colaAnalisis)); } catch (e) {}
}
function colaAnalisisPush(contactId, nombre) {
  try {
    if (colaAnalisis.some(x => x.contactId === contactId && !x.done)) return;
    colaAnalisis.push({ contactId: contactId, nombre: nombre || '', ts: Date.now(), done: false });
    if (colaAnalisis.length > 500) colaAnalisis = colaAnalisis.slice(-500);
    colaAnalisisSave();
  } catch (e) { console.error('colaAnalisisPush: ' + e.message); }
}
app.get('/cola_analisis', (req, res) => {
  if (!process.env.REWARDS_HITOS_KEY || req.query.key !== process.env.REWARDS_HITOS_KEY) {
    return res.status(403).json({ ok: false });
  }
  res.json({ ok: true, pendientes: colaAnalisis.filter(x => !x.done) });
});
app.post('/cola_analisis/done', (req, res) => {
  if (!process.env.REWARDS_HITOS_KEY || (req.body || {}).key !== process.env.REWARDS_HITOS_KEY) {
    return res.status(403).json({ ok: false });
  }
  const ids = new Set(((req.body || {}).contactIds) || []);
  colaAnalisis.forEach(x => { if (ids.has(x.contactId)) x.done = true; });
  colaAnalisisSave();
  res.json({ ok: true, marcados: ids.size });
});

// ===== LINEA INSTANTANEA (v8.43.0) =====
// Webhook "Message Received" de respond.io -> cola en memoria que la Mac drena cada ~15s
// (segunda-linea-daemon.mjs). Elimina la espera de 5 min del cron: el AI contesta al momento.
// La cola es efimera (se pierde en redeploy): aceptable, el cron de 5 min sigue de red de seguridad.
let colaMensajes = [];
app.post('/webhook/mensaje-entrante', (req, res) => {
  try {
    const ev = req.body || {};
    const contactId = (ev.contact && ev.contact.id) || ev.contact_id || null;
    const msg = ev.message || {};
    const traffic = msg.traffic || ev.traffic || '';
    if (contactId && traffic !== 'outgoing') {
      const texto = (msg.message && (msg.message.text || msg.message.caption)) || '';
      const nombre = (ev.contact && ((ev.contact.firstName || '') + ' ' + (ev.contact.lastName || '')).trim()) || '';
      // NOTAS DE VOZ (28-ago, Daniel: "puede ser una orden del cliente que no recibimos"):
      // se transcriben con Whisper y el texto entra a la cola como cualquier mensaje, para que
      // el AI atienda el pedido en vez de escalarlo. Async: no bloquea la respuesta del webhook.
      const adj = msg.message && msg.message.attachment;
      const esAudio = adj && (adj.type === 'audio' || /^audio\//.test(adj.mimeType || '') || /(ogg|mp3|m4a|wav|opus)$/i.test(adj.ext || ''));
      if (esAudio && adj.url) {
        (async () => {
          try {
            const t = await transcribeAudio(adj.url);
            const linea = t ? ('\u{1F3A4} NOTA DE VOZ (transcrita): ' + t) : '\u{1F3A4} NOTA DE VOZ que no se pudo transcribir — escalar para que un humano la escuche';
            const y2 = colaMensajes.find(x => x.contactId === contactId);
            if (y2) { y2.ts = Date.now(); y2.texto = linea.slice(0, 600); }
            else colaMensajes.push({ contactId, nombre, texto: linea.slice(0, 600), ts: Date.now() });
            console.log('[voz] contacto ' + contactId + ': ' + String(t || '(sin transcripcion)').slice(0, 120));
          } catch (e) { console.error('[voz] ' + e.message); }
        })();
      }
      const ya = colaMensajes.find(x => x.contactId === contactId);
      if (ya) { ya.ts = Date.now(); ya.texto = String(texto).slice(0, 200); }
      else colaMensajes.push({ contactId, nombre, texto: String(texto).slice(0, 200), ts: Date.now() });
      if (colaMensajes.length > 200) colaMensajes = colaMensajes.slice(-200);
      console.log('[mensaje-entrante] contacto ' + contactId + ' en cola (' + colaMensajes.length + ')');
    }
    res.json({ ok: true });
  } catch (e) { console.error('mensaje-entrante: ' + e.message); res.json({ ok: false }); }
});
// Webhook "New Comment" de respond.io (27-ago): comentarios internos del EQUIPO se atienden
// AL INSTANTE por la misma linea (feedback de mejoras/quejas al AI, o instrucciones operativas).
// Se ignoran los comentarios del propio AI (empiezan con el robot) para no ciclar.
app.post('/webhook/comentario-interno', (req, res) => {
  try {
    const ev = req.body || {};
    const contactId = (ev.contact && ev.contact.id) || ev.contact_id || null;
    const c = ev.comment || ev.data || {};
    const texto = String(c.text || c.content || ev.text || '').trim();
    const autor = (c.user && ((c.user.firstName || '') + ' ' + (c.user.lastName || '')).trim()) || (ev.user && ev.user.firstName) || 'equipo';
    // Ignorar los comentarios del PROPIO AI (empiezan con su marca) — 27-ago: ~20 corridas
    // se gastaron en auto-disparos porque solo se filtraba el robot.
    const esDelAI = /^(\u{1F916}|\u{1F6A8}|\u{1F4E6}|\u{1F4A1}|\u{1FA6A}|\u{23F3}|\u{1F4C4}|\u{1F50D})/u.test(texto) || /^AI( |:)/i.test(texto);
    if (contactId && texto && !esDelAI) {
      const marcado = '[COMENTARIO INTERNO de ' + autor + ']: ' + texto.slice(0, 400);
      const ya = colaMensajes.find(x => x.contactId === contactId);
      if (ya) { ya.ts = Date.now(); ya.texto = (ya.texto + ' | ' + marcado).slice(-600); }
      else colaMensajes.push({ contactId, nombre: (ev.contact && ev.contact.firstName) || '', texto: marcado, ts: Date.now() });
      if (colaMensajes.length > 200) colaMensajes = colaMensajes.slice(-200);
      console.log('[comentario-interno] ' + autor + ' en contacto ' + contactId + ' -> cola');
    }
    res.json({ ok: true });
  } catch (e) { console.error('comentario-interno: ' + e.message); res.json({ ok: false }); }
});

// Transcripción a demanda: el AI manda {url} de una nota de voz y recibe el texto.
app.post('/transcribir', async (req, res) => {
  if (!process.env.REWARDS_HITOS_KEY || (req.body || {}).key !== process.env.REWARDS_HITOS_KEY) return res.status(403).json({ ok: false });
  const url = (req.body || {}).url;
  if (!url) return res.status(400).json({ ok: false, error: 'falta url' });
  try {
    const texto = await transcribeAudio(url);
    res.json({ ok: !!texto, texto: texto || null });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

app.get('/webhook/cola-mensajes', (req, res) => {
  if (!process.env.REWARDS_HITOS_KEY || req.query.key !== process.env.REWARDS_HITOS_KEY) {
    return res.status(403).json({ ok: false });
  }
  const items = colaMensajes; colaMensajes = [];
  res.json({ ok: true, items });
});

app.post('/webhook/conversation-closed', async (req, res) => {
  console.log('\n[' + new Date().toISOString() + '] === WEBHOOK RECEIVED (v7) ===');
  console.log('Body:', JSON.stringify(req.body, null, 2));

  res.json({ received: true });

  const contactId = extractContactId(req.body);
  const contactName = extractContactName(req.body);

  if (!contactId) {
    console.error('Could not extract contact_id from payload.');
    return;
  }

  colaAnalisisPush(contactId, contactName);   // v8.37: cola para la Mac

  console.log('Analyzing conversation for contact: ' + contactId + ' (' + contactName + ')');

  try {
    // Fetch messages and contact info in parallel
    const [messagesResponse, contactResponse] = await Promise.all([
      fetch(
        'https://api.respond.io/v2/contact/id:' + contactId + '/message/list?limit=50',
        {
          headers: {
            'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
            'Content-Type': 'application/json'
          }
        }
      ),
      fetch(
        'https://api.respond.io/v2/contact/id:' + contactId,
        {
          headers: {
            'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
            'Content-Type': 'application/json'
          }
        }
      )
    ]);

    if (!messagesResponse.ok) {
      const errorText = await messagesResponse.text();
      console.error('Respond.io messages API error: ' + messagesResponse.status + ' - ' + errorText);
      return;
    }

    const messagesData = await messagesResponse.json();
    const messages = messagesData.data || messagesData.items || [];

    if (messages.length === 0) {
      console.log('No messages found for this contact');
      return;
    }

    // Get assignee (for reference only)
    let assignee = null;
    if (contactResponse.ok) {
      const contactData = await contactResponse.json();
      assignee = contactData.assignee || null;
    }

    // Extract agents who ACTUALLY sent messages DENTRO de la ventana de evaluacion.
    // Sin esta ventana se calificaba a gente por mensajes de meses atras (ver BUGFIX en
    // extractAgentsFromMessages). EVAL_WINDOW_DAYS=0 desactiva el filtro.
    const evalWindowDays = Number(process.env.EVAL_WINDOW_DAYS || 7);
    const sinceMs = evalWindowDays > 0 ? Date.now() - evalWindowDays * 86400000 : null;
    const { humans, bots, all: allAgents } = extractAgentsFromMessages(messages, sinceMs);
    const humanNames = humans.map(a => a.name);
    const botNames = bots.map(a => a.name);
    const allNames = allAgents.map(a => a.name).join(', ') || 'Sin agente';

    console.log('Agents detected - Humans: ' + humanNames.join(', ') + ' | Bots: ' + botNames.join(', '));

    // v7.1: Format messages including internal notes and voice transcriptions
    const formattedMessagesArray = await Promise.all(messages.map(async (msg) => {
      const traffic = msg.traffic || msg.type;
      const messageObj = msg.message || {};
      const attachment = messageObj.attachment || {};

      // v7.1: Check if this is a voice/audio message
      const isAudio = (
        messageObj.type === 'attachment' &&
        attachment.type === 'audio' &&
        attachment.url
      );

      // v7: Detect internal notes/comments
      if (traffic === 'internal' || msg.internal === true || msg.messageType === 'internal') {
        const sender = msg.sender;
        const uid = sender?.userId;
        const known = uid ? AGENT_MAP[uid] : null;
        const senderName = known ? known.name : (sender?.name || 'Equipo');
        const text = msg.text || messageObj.text || msg.body || '[nota sin texto]';
        return 'NOTA INTERNA de ' + senderName + ': ' + text;
      }

      if (traffic === 'incoming') {
        // v7.1: Transcribe voice messages from clients
        if (isAudio) {
          const transcription = await transcribeAudio(attachment.url);
          if (transcription) {
            return 'CLIENTE [mensaje de voz]: ' + transcription;
          }
          return 'CLIENTE: [mensaje de voz - no se pudo transcribir]';
        }
        const text = msg.text || messageObj.text || msg.body || '[media/attachment]';
        return 'CLIENTE: ' + text;
      } else if (traffic === 'outgoing') {
        const sender = msg.sender;
        const uid = sender?.userId;
        const source = sender?.source;
        let label = 'AGENTE';

        if (source === 'ai_agent') {
          const known = AGENT_MAP[uid];
          label = '[BOT] ' + (known ? known.name : 'Agente Virtual');
        } else if (source === 'user' && uid) {
          const known = AGENT_MAP[uid];
          label = known ? known.name : 'Agente #' + uid;
        } else if (source === 'workflow') {
          label = '[WORKFLOW]';
        }

        // v7.1: Transcribe voice messages from agents too
        if (isAudio) {
          const transcription = await transcribeAudio(attachment.url);
          if (transcription) {
            return label + ' [mensaje de voz]: ' + transcription;
          }
          return label + ': [mensaje de voz - no se pudo transcribir]';
        }
        const text = msg.text || messageObj.text || msg.body || '[media/attachment]';
        return label + ': ' + text;
      }

      // Fallback for any other message type
      const text = msg.text || messageObj.text || msg.body || '';
      if (text) return '[SISTEMA]: ' + text;
      return null;
    }));
    const formattedMessages = formattedMessagesArray.filter(m => m !== null).join('\n');

    const channel = messages[0]?.channelType || messages[0]?.channel || 'desconocido';
    const link = 'https://app.respond.io/space/379868/inbox/' + contactId;

    const hasBotAgent = bots.length > 0;
    const hasHumanAgent = humans.length > 0;

    // v7: Build conversation identifier
    const conversacionId = contactName + ' - ' + channel + ' (#' + contactId + ')';

    // v7: Build agent roles info
    const agentRolesInfo = humans.map(a => {
      const role = getAgentRole(a.name);
      const roleDesc = {
        'owner': 'due√±o del negocio',
        'admin': 'administraci√≥n (facturaci√≥n, cobranza, log√≠stica)',
        'sales': 'ventas y atenci√≥n al cliente'
      };
      return a.name + ' (' + (roleDesc[role] || role) + ')';
    }).join(', ');

    // v7: Build evaluation instructions per agent
    let humanEvalInstructions = '';
    if (hasHumanAgent) {
      const agentEntries = humanNames.map(name => {
        const role = getAgentRole(name);
        return `  {
    "nombre_agente": "${name}",
    "rol": "${role}",
    "atencion_cliente": 8,
    "conocimiento_solucion": 7,
    "proactividad": 8,
    "cierre_resultado": 7,
    "calificacion_general": 7.5,
    "feedback": "Maximo 2 oraciones: 1 cosa bien + 1 a mejorar (si aplica). Si hizo bien su trabajo, solo reconocerlo."
  }`;
      }).join(',\n');

      humanEvalInstructions = `
"evaluaciones_individuales": [
${agentEntries}
]`;
    }

    let botEvalInstructions = '';
    if (hasBotAgent) {
      botEvalInstructions = `
"evaluacion_bot": {
  "precision_respuestas": 8,
  "manejo_consulta": 7,
  "transicion_humano": 9,
  "tono_comunicacion": 8,
  "calificacion_general": 8,
  "feedback_bot": "Que hizo bien y que deberia mejorar el bot.",
  "mejoras_sugeridas": ["Mejora 1", "Mejora 2"]
}`;
    }

    // v7: COMPLETELY REWRITTEN PROMPT
    const analysisPrompt = `Eres el evaluador de servicio al cliente de Filmorent, un negocio de RENTA de equipo de cine y fotografia en Monterrey, Mexico.

=== PASO 1: ENTENDER LA CONVERSACION COMPLETA ===

Lee TODA la conversacion de principio a fin. Entiende:
- Que necesitaba el cliente
- Como respondio el equipo EN CONJUNTO
- Cual fue el resultado final
- Las NOTAS INTERNAS son instrucciones del due√±o (Daniel Alonso) al equipo. Seguirlas es CORRECTO.

=== PASO 2: REGLAS CRITICAS DE EVALUACION ===

REGLA 1 - TRABAJO EN EQUIPO: Los agentes trabajan como EQUIPO. Si un agente solo envio un mensaje de cierre cortes o de seguimiento, eso es POSITIVO y demuestra trabajo en equipo. NO penalizar porque "su participacion fue limitada" - cada mensaje cuenta.

REGLA 2 - RAPIDEZ ES BUENA: Enviar cotizacion o informacion rapido es BUENO para el negocio. NUNCA penalizar por "enviar cotizacion antes de explicar" o "no dar contexto previo". La rapidez cierra rentas.

REGLA 3 - ROLES DIFERENTES: Cada agente tiene un ROL diferente:
${agentRolesInfo}
- Agentes de ADMIN: Evaluar en facturacion, cobranza, logistica. NO penalizar por "no conocer equipos".
- Agentes de VENTAS: Evaluar en atencion, conocimiento de equipos, cierre de rentas.
- El DUE√ëO: Generalmente da instrucciones internas, no evaluarlo a menos que interactue con el cliente.

REGLA 4 - NOTAS INTERNAS: Los mensajes marcados "NOTA INTERNA" son instrucciones del due√±o al equipo. Si un agente sigue una instruccion interna (ej: "ofrecele la ZVE10"), eso es CORRECTO. No penalizar por "introducir informacion no solicitada" cuando fue una instruccion.

REGLA 5 - ENFOCARSE EN LO IMPORTANTE: Evalua lo que REALMENTE importa para el negocio:
- Se atendio bien al cliente?
- Se respondieron TODAS sus preguntas?
- Se busco resolver su necesidad?
- Se contribuyo a concretar la renta?
NO buscar defectos artificiales. Si el agente hizo bien su trabajo, di que lo hizo bien.

REGLA 6 - FEEDBACK UTIL: El feedback debe ser ACCIONABLE y ENFOCADO. Maximo 2 oraciones: 1 cosa positiva + 1 area de mejora (SOLO si realmente hay algo importante que mejorar). Si el agente hizo bien su trabajo, no inventes criticas.

=== DATOS DE LA CONVERSACION ===

Conversacion: ${conversacionId}
Canal: ${channel}
Agentes humanos: ${humanNames.join(', ') || 'Ninguno'}
Roles: ${agentRolesInfo || 'N/A'}
Bot: ${botNames.join(', ') || 'Ninguno'}

=== CONVERSACION ===
${formattedMessages}

=== PASO 3: EVALUAR ===

Criterios por agente humano (1-10):
1. atencion_cliente: Cordialidad, profesionalismo, respondio todas las preguntas del cliente? (Adaptado al ROL del agente)
2. conocimiento_solucion: Demostro conocimiento relevante a su rol y busco soluciones? (Para ventas: equipos. Para admin: procesos administrativos)
3. proactividad: Fue rapido, ofrecio alternativas, dio seguimiento, tomo iniciativa?
4. cierre_resultado: Contribuyo a concretar la renta o resolver la necesidad del cliente?

TAGS A EVALUAR:
1. "consulta-compra" - Cliente pregunta por COMPRAR equipo (Filmorent solo renta).
2. "equipo-no-disponible" - Equipo no disponible (no existe O ya rentado).
3. "incidencia" - Problema, queja, equipo da√±ado, entrega tarde.
4. "renta-perdida" - Cliente queria rentar pero NO se concreto. Causa: "precio", "sin_respuesta_cliente", "tardanza_respuesta", "fechas", "ubicacion", "otro".

Responde UNICAMENTE con JSON valido (sin markdown, sin backticks, solo JSON puro):
{
  "conversacion_id": "${conversacionId}",
  "tags": ["tag1"],
  "causa_renta_perdida": "causa o null",
  "resumen": "Resumen de 3-5 oraciones: que pidio el cliente, que paso, y cual fue el RESULTADO FINAL.",
  "resultado": "concretada | perdida | pendiente | no_aplica",
  "equipos_solicitados": [{"nombre": "equipo", "disponible": true}],
  ${hasHumanAgent ? humanEvalInstructions + ',' : '"evaluaciones_individuales": [],'}
  ${hasBotAgent ? botEvalInstructions : '"evaluacion_bot": null'}
}`;

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-5',
      max_tokens: 4000,
      messages: [{ role: 'user', content: analysisPrompt }]
    });

    const analysisText = claudeText(claudeResponse);
    console.log('Claude analysis: ' + analysisText);

    let tagsToApply = [];
    let causaRentaPerdida = null;
    let resumen = '';
    let equipos = [];
    let evaluacionesIndividuales = [];
    let evaluacionBot = null;
    let resultado = '';
    let parsedConversacionId = conversacionId;

    try {
      let cleanJson = analysisText;
      if (cleanJson.startsWith('```')) {
        cleanJson = cleanJson.replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '');
      }
      const parsed = JSON.parse(cleanJson);
      tagsToApply = parsed.tags || [];
      causaRentaPerdida = parsed.causa_renta_perdida || null;
      resumen = parsed.resumen || '';
      equipos = parsed.equipos_solicitados || [];
      evaluacionesIndividuales = parsed.evaluaciones_individuales || [];
      evaluacionBot = parsed.evaluacion_bot || null;
      resultado = parsed.resultado || '';
      parsedConversacionId = parsed.conversacion_id || conversacionId;
    } catch (e) {
      console.log('JSON parse error: ' + e.message);
      const validTags = ['consulta-compra', 'equipo-no-disponible', 'incidencia', 'renta-perdida'];
      validTags.forEach(tag => {
        if (analysisText.toLowerCase().includes(tag)) {
          tagsToApply.push(tag);
        }
      });
      resumen = 'Error parsing JSON';
    }

    const validTags = ['consulta-compra', 'equipo-no-disponible', 'incidencia', 'renta-perdida'];
    tagsToApply = tagsToApply.filter(tag => validTags.includes(tag));

    if (tagsToApply.includes('renta-perdida') && causaRentaPerdida) {
      tagsToApply.push('renta-perdida:' + causaRentaPerdida);
    }

    // Apply tags to Respond.io
    if (tagsToApply.length > 0) {
      const tagResponse = await fetch(
        'https://api.respond.io/v2/contact/id:' + contactId + '/tag',
        {
          method: 'POST',
          headers: {
            'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ tags: tagsToApply })
        }
      );
      if (!tagResponse.ok) {
        const errorText = await tagResponse.text();
        console.error('Failed to apply tags: ' + tagResponse.status + ' - ' + errorText);
      } else {
        console.log('Tags applied: ' + tagsToApply.join(', '));
      }
    }

    // v7: Add role to each evaluation
    evaluacionesIndividuales = evaluacionesIndividuales.map(ev => ({
      ...ev,
      rol: ev.rol || getAgentRole(ev.nombre_agente)
    }));

    // Log to Google Sheets - v7 format
    await logToGoogleSheets({
      version: 'v7.1',
      fecha: new Date().toISOString(),
      contactId: contactId,
      nombre: contactName,
      conversacion_id: parsedConversacionId,
      tags: tagsToApply.join(', '),
      causa_renta_perdida: causaRentaPerdida || '',
      num_mensajes: messages.length,
      canal: channel,
      resumen: resumen,
      link_conversacion: link,
      conversacion_completa: formattedMessages.substring(0, 45000),
      resultado: resultado,
      equipos_solicitados: equipos,
      // v7: Individual evaluations with roles and 4 criteria
      evaluaciones_individuales: evaluacionesIndividuales,
      evaluacion_bot: evaluacionBot,
      agentes_humanos: humans.map(a => ({ nombre: a.name, email: a.email, userId: a.userId })),
      agentes_bot: bots.map(a => ({ nombre: a.name, userId: a.userId })),
      agentes_todos: allNames,
      assignee: assignee ? (assignee.firstName + ' ' + (assignee.lastName || '')).trim() : 'Sin asignar'
    });

    const califBot = evaluacionBot ? evaluacionBot.calificacion_general : 'N/A';
    const califHumanos = evaluacionesIndividuales.map(e => e.nombre_agente + '=' + e.calificacion_general).join(', ');
    console.log('=== DONE: contact=' + contactId + ', calif_humanos=[' + califHumanos + '], calif_bot=' + califBot + ' ===\n');

  } catch (error) {
    console.error('Error:', error.message);
  }
});

// ============================================================
// v7.2: AUTO-SUMMARY on Conversation Opened
// When a conversation reopens, generate an AI summary of ALL
// previous messages and post it as an internal comment so
// the agent has immediate context.
// ============================================================

app.post('/webhook/conversation-opened', async (req, res) => {
  console.log('\n[' + new Date().toISOString() + '] === CONVERSATION OPENED (v7.2) ===');

  res.json({ received: true });

  const contactId = extractContactId(req.body);
  const contactName = extractContactName(req.body);

  if (!contactId) {
    console.error('conversation-opened: Could not extract contact_id');
    return;
  }

  console.log('Generating summary for: ' + contactId + ' (' + contactName + ')');

  try {
    // Fetch ALL messages for this contact (up to 200 for full context)
    const messagesResponse = await fetch(
      'https://api.respond.io/v2/contact/id:' + contactId + '/message/list?limit=200',
      {
        headers: {
          'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
          'Content-Type': 'application/json'
        }
      }
    );

    if (!messagesResponse.ok) {
      console.error('Failed to fetch messages: ' + messagesResponse.status);
      return;
    }

    const messagesData = await messagesResponse.json();
    const messages = messagesData.data || messagesData.items || [];

    // Only generate summary if there are enough previous messages
    if (messages.length < 3) {
      console.log('Too few messages (' + messages.length + '), skipping summary');
      return;
    }

    // Format messages (simplified - no audio transcription for speed)
    const formattedMessages = messages.map(msg => {
      const traffic = msg.traffic || msg.type;
      const messageObj = msg.message || {};
      const text = msg.text || messageObj.text || msg.body || '';

      if (traffic === 'internal' || msg.internal === true) {
        const sender = msg.sender;
        const uid = sender?.userId;
        const known = uid ? AGENT_MAP[uid] : null;
        const senderName = known ? known.name : 'Equipo';
        return 'NOTA INTERNA de ' + senderName + ': ' + (text || '[nota]');
      }

      if (traffic === 'incoming') {
        return 'CLIENTE: ' + (text || '[media]');
      } else if (traffic === 'outgoing') {
        const sender = msg.sender;
        const uid = sender?.userId;
        const source = sender?.source;
        let label = 'AGENTE';

        if (source === 'ai_agent') {
          const known = AGENT_MAP[uid];
          label = '[BOT] ' + (known ? known.name : 'Bot');
        } else if (source === 'user' && uid) {
          const known = AGENT_MAP[uid];
          label = known ? known.name : 'Agente';
        } else if (source === 'workflow') {
          label = '[WORKFLOW]';
        }
        return label + ': ' + (text || '[media]');
      }
      return null;
    }).filter(m => m !== null).join('\n');

    if (!formattedMessages || formattedMessages.length < 50) {
      console.log('Not enough text content to summarize');
      return;
    }

    // Generate summary with Claude
    const summaryPrompt = `Eres asistente de Filmorent (renta de equipo de cine/foto en Monterrey).

Genera un resumen BREVE y UTIL para el agente que va a atender a este cliente que vuelve a escribir. El agente necesita saber rapidamente:

1. **Quien es**: Nombre del cliente y canal de comunicacion
2. **Que ha pedido antes**: Equipos solicitados, fechas, proyectos mencionados
3. **Estado actual**: Se concreto alguna renta? Quedo algo pendiente? Hubo algun problema?
4. **Datos clave**: Precios cotizados, acuerdos hechos, condiciones especiales
5. **Lo mas reciente**: Que paso en la ultima conversacion

IMPORTANTE:
- Maximo 8-10 lineas
- Se directo y practico, esto es para que el agente sepa que paso SIN tener que leer todo
- Si hubo multiples conversaciones/ciclos, resume TODOS, no solo el ultimo
- Usa formato simple con vi√±etas
- Escribe en espa√±ol

=== HISTORIAL COMPLETO DEL CLIENTE: ${contactName} ===
${formattedMessages}

=== FIN DEL HISTORIAL ===

Genera el resumen ahora:`;

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-5',
      max_tokens: 1000,
      messages: [{ role: 'user', content: summaryPrompt }]
    });

    const summary = claudeText(claudeResponse);
    console.log('Summary generated: ' + summary.substring(0, 200) + '...');

    // Post summary as internal comment via Respond.io API
    const commentResponse = await fetch(
      'https://api.respond.io/v2/contact/id:' + contactId + '/comment',
      {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          text: 'ü§ñ RESUMEN AUTOMATICO (IA)\n‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ\n' + summary + '\n‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ\nüìù Resumen generado al reabrir conversacion'
        })
      }
    );

    if (commentResponse.ok) {
      console.log('Summary posted as internal comment for contact ' + contactId);
    } else {
      const errorText = await commentResponse.text();
      console.error('Failed to post comment: ' + commentResponse.status + ' - ' + errorText);
    }

  } catch (error) {
    console.error('conversation-opened error: ' + error.message);
  }
});


// ============================================================
// v7.3: CALL ENDED ‚Äî analiza la transcripcion de una llamada
// (Respond.io Voice AI / llamadas) y aplica tags. Si no hay
// transcript (llamada perdida o sin grabacion), se omite.
// ============================================================

function extractCallTranscript(body) {
  return (
    body?.call?.transcript ||
    body?.data?.call?.transcript ||
    body?.transcript ||
    body?.data?.transcript ||
    null
  );
}

function extractCallSummary(body) {
  return (
    body?.call?.summary ||
    body?.data?.call?.summary ||
    body?.summary ||
    body?.data?.summary ||
    body?.call?.aiSummary ||
    body?.data?.call?.aiSummary ||
    null
  );
}

function extractCallMeta(body) {
  const call = body?.call || body?.data?.call || body || {};
  return {
    callId: call?.id || call?.callId || null,
    duration: call?.duration || call?.durationSeconds || null,
    status: call?.status || null,
    direction: call?.direction || null
  };
}

app.post('/webhook/call-ended', async (req, res) => {
  console.log('\n[' + new Date().toISOString() + '] === CALL ENDED (v7.3) ===');
  console.log('Body:', JSON.stringify(req.body, null, 2));

  res.json({ received: true });

  const contactId = extractContactId(req.body);
  const contactName = extractContactName(req.body);
  const transcript = extractCallTranscript(req.body);
  const summary = extractCallSummary(req.body);
  const meta = extractCallMeta(req.body);

  console.log('Call meta: ' + JSON.stringify(meta) + ', contact=' + contactId + ' (' + contactName + ')');

  if (!contactId) {
    console.error('call-ended: Could not extract contact_id');
    return;
  }

  if (!transcript) {
    // LLAMADA PERDIDA (v8.44.0, opcion A de Daniel 27-ago): sin transcripcion + sin duracion real
    // en una llamada entrante = nadie contesto. Se mete a la cola de la linea instantanea para que
    // el AI le escriba por TEXTO al momento ("Vimos tu llamada 📞...").
    const perdida = meta.direction !== 'outgoing' &&
      ((meta.duration || 0) <= 3 || /miss|no.?answer|unanswer|cancel|reject/i.test(String(meta.status || '')));
    if (perdida) {
      const ya = colaMensajes.find(x => x.contactId === contactId);
      const texto = '[LLAMADA PERDIDA — el cliente llamo por WhatsApp y nadie contesto]';
      if (ya) { ya.ts = Date.now(); ya.texto = texto; }
      else colaMensajes.push({ contactId, nombre: contactName || '', texto, ts: Date.now() });
      console.log('call-ended: llamada PERDIDA de ' + contactId + ' -> cola linea instantanea');
    } else {
      console.log('call-ended: No transcript for call ' + meta.callId + ' (sin grabacion). Skipping.');
    }
    return;
  }

  try {
    const summarySection = summary ? ('\n\nRESUMEN AI:\n' + summary) : '';

    const analysisPrompt = 'Analiza la siguiente transcripcion de una LLAMADA telefonica entrante a Filmorent (renta de equipo audiovisual en Monterrey). Determina si aplica alguno de estos tags:\n\n' +
      '1. "consulta-compra" - El cliente pregunto por COMPRAR equipo (no rentar). Filmorent solo renta, no vende.\n' +
      '2. "equipo-no-disponible" - El cliente pregunto por equipo que probablemente NO esta en el catalogo de renta.\n' +
      '3. "incidencia" - El cliente reporto un problema, queja, equipo danado, entrega tarde, cobro incorrecto o situacion negativa.\n' +
      '4. "llamada-cotizacion" - La llamada fue una cotizacion exitosa donde el cliente dejo datos (nombre, equipo, fechas, contacto) y se le ofrecio mandar cotizacion por WhatsApp.\n\n' +
      'TRANSCRIPCION:\n' + transcript + summarySection + '\n\n' +
      'Responde UNICAMENTE con un JSON valido en este formato exacto, sin texto adicional:\n' +
      '{"tags": ["tag1", "tag2"]}\n\n' +
      'Si no aplica ningun tag, responde: {"tags": []}\n' +
      'Solo incluye tags que CLARAMENTE apliquen basado en la llamada.';

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-5',
      max_tokens: 150,
      messages: [{ role: 'user', content: analysisPrompt }]
    });

    const analysisText = claudeText(claudeResponse);
    console.log('Claude call analysis: ' + analysisText);

    let tagsToApply = [];
    try {
      let cleanJson = analysisText;
      if (cleanJson.startsWith('```')) {
        cleanJson = cleanJson.replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '');
      }
      const parsed = JSON.parse(cleanJson);
      tagsToApply = parsed.tags || [];
    } catch (e) {
      const vt = ['consulta-compra', 'equipo-no-disponible', 'incidencia', 'llamada-cotizacion'];
      vt.forEach(function (tag) {
        if (analysisText.toLowerCase().includes(tag)) tagsToApply.push(tag);
      });
    }

    const validTags = ['consulta-compra', 'equipo-no-disponible', 'incidencia', 'llamada-cotizacion'];
    tagsToApply = tagsToApply.filter(function (tag) { return validTags.includes(tag); });

    if (tagsToApply.length > 0) {
      const tagResponse = await fetch(
        'https://api.respond.io/v2/contact/id:' + contactId + '/tag',
        {
          method: 'POST',
          headers: {
            'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ tags: tagsToApply })
        }
      );
      if (!tagResponse.ok) {
        const errorText = await tagResponse.text();
        console.error('call-ended: Failed to apply tags: ' + tagResponse.status + ' - ' + errorText);
      } else {
        console.log('call-ended: Tags applied: ' + tagsToApply.join(', '));
      }
    } else {
      console.log('call-ended: No tags to apply');
    }

    console.log('=== DONE CALL: contact=' + contactId + ', call=' + meta.callId + ', tags=' + JSON.stringify(tagsToApply) + ' ===\n');
  } catch (error) {
    console.error('call-ended error: ' + error.message);
  }
});


// ‚îÄ‚îÄ v7.4 BACKFILL (30-jul-2026) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
// Rellena huecos del Log de Conversaciones (ej. 15-jun‚Üí13-jul 2026, cuando el
// modelo Claude descontinuado rompia el analisis). READ-ONLY: solo ENUMERA los
// contactId cuyo "ultimo mensaje entrante" cae en el rango, via POST /contact/list
// (API v2). No reprocesa ni escribe nada aqui ‚Äî de eso se encarga el script local
// webhook-server/backfill.py, que es resumible (no duplica filas al re-correr).
//
// Protegido: requiere la env BACKFILL_TOKEN. Si NO esta seteada, el endpoint esta
// DESHABILITADO (403). Setea BACKFILL_TOKEN en Render para habilitarlo, corre el
// backfill, y luego puedes quitarla.
//   GET /backfill?token=XXX&from=2026-06-15&to=2026-07-13
//   opcionales: field, valueFormat (ms|iso|s|datetime), probe=1, max (tope, def 3000)
const BACKFILL_TZ = 'America/Monterrey'; // UTC-6 fijo (Monterrey no observa DST)
const BACKFILL_FIELD_CANDIDATES = [
  'lastIncomingMessageTime', 'lastIncomingMessage', 'latestIncomingMessageTime',
  'lastInboundMessageTime', 'lastContactTime', 'lastMessageTime', 'lastInteraction'
];
const BACKFILL_FMT_CANDIDATES = ['ms', 'iso', 's', 'datetime'];

// pagination.next puede ser un cursor pelon o una URL con ?cursorId=... ‚Äî extrae el cursor.
function backfillNextCursor(next) {
  if (!next) return null;
  const s = String(next);
  const m = s.match(/cursorId=([^&]+)/);
  return m ? decodeURIComponent(m[1]) : s;
}

function backfillFmtValue(dateStr, which, fmt) {
  // dateStr = 'YYYY-MM-DD'; which='from' => inicio de dia, 'to' => fin de dia (MTY, UTC-6)
  if (fmt === 'datetime') return which === 'to' ? dateStr + ' 23:59' : dateStr + ' 00:00';
  const iso = (which === 'to' ? dateStr + 'T23:59:59' : dateStr + 'T00:00:00') + '-06:00';
  const d = new Date(iso);
  if (fmt === 'ms') return String(d.getTime());
  if (fmt === 's') return String(Math.floor(d.getTime() / 1000));
  return d.toISOString(); // iso
}

async function backfillListPage(field, value, cursorId, limit) {
  let url = 'https://api.respond.io/v2/contact/list?limit=' + (limit || 100);
  if (cursorId) url += '&cursorId=' + encodeURIComponent(cursorId);
  const body = {
    search: '', // la API la requiere presente aunque sea vacia (400 si falta)
    timezone: BACKFILL_TZ,
    filter: { $and: [ { category: 'contactField', field: field, operator: 'isTimestampBetween', value: value } ] }
  };
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  const text = await resp.text();
  let json = null; try { json = JSON.parse(text); } catch (e) {}
  const items = (json && (json.items || json.data)) || [];
  const next = json && json.pagination && json.pagination.next;
  return { status: resp.status, items: Array.isArray(items) ? items : [], next: next || null, text: text };
}

// Descubre la combinacion (field, valueFormat) que la API acepta (status 200 con items>0).
async function backfillDiscover(from, to, forceField, forceFmt) {
  const fields = (forceField ? [forceField] : BACKFILL_FIELD_CANDIDATES);
  const fmts = (forceFmt ? [forceFmt] : BACKFILL_FMT_CANDIDATES);
  const log = [];
  let fallback = null; // combo que dio 200 aunque con 0 items
  for (const field of fields) {
    for (const fmt of fmts) {
      const value = { from: backfillFmtValue(from, 'from', fmt), to: backfillFmtValue(to, 'to', fmt) };
      try {
        const r = await backfillListPage(field, value, null, 1);
        log.push({ field: field, fmt: fmt, status: r.status, items: r.items.length,
          err: r.status >= 400 ? String(r.text || '').slice(0, 250) : undefined });
        if (r.status === 200 && r.items.length > 0) return { field: field, fmt: fmt, value: value, log: log };
        if (r.status === 200 && !fallback) fallback = { field: field, fmt: fmt, value: value };
      } catch (e) { log.push({ field: field, fmt: fmt, error: e.message }); }
    }
  }
  if (fallback) return { field: fallback.field, fmt: fallback.fmt, value: fallback.value, log: log, empty: true };
  return { field: null, fmt: null, value: null, log: log };
}

app.get('/backfill', async (req, res) => {
  if (!BACKFILL_TOKEN) return res.status(403).json({ error: 'backfill deshabilitado: setea BACKFILL_TOKEN en el env para habilitarlo' });
  if (req.query.token !== BACKFILL_TOKEN) return res.status(401).json({ error: 'token invalido' });
  const from = req.query.from, to = req.query.to;
  if (!from || !to) return res.status(400).json({ error: 'faltan from y to (YYYY-MM-DD)' });

  // DIAGNOSTICO: ?dump=1 -> lista 1 contacto (por createdAt, que si funciona) y devuelve
  // el objeto completo para descubrir los nombres de campo reales (ej. el de ultimo mensaje).
  if (req.query.dump === '1') {
    try {
      const val = { from: backfillFmtValue(from, 'from', 'datetime'), to: backfillFmtValue(to, 'to', 'datetime') };
      const r = await backfillListPage('createdAt', val, null, 1);
      const item = (r.items && r.items[0]) || null;
      let full = null;
      if (item && item.id) {
        const gr = await fetch('https://api.respond.io/v2/contact/id:' + item.id, {
          headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' }
        });
        try { full = await gr.json(); } catch (e) { full = null; }
      }
      return res.json({
        dump: true, listStatus: r.status,
        listItemKeys: item ? Object.keys(item) : null,
        listItem: item,
        fullContactKeys: full ? Object.keys(full) : null,
        fullContact: full
      });
    } catch (e) { return res.status(500).json({ dumpError: e.message }); }
  }

  try {
    const disc = await backfillDiscover(from, to, req.query.field, req.query.valueFormat);
    if (req.query.probe === '1') return res.json({ probe: true, chosen: { field: disc.field, fmt: disc.fmt }, log: disc.log, value: disc.value });
    if (!disc.field) return res.status(502).json({ error: 'ningun field/formato aceptado por la API', log: disc.log });

    const max = parseInt(req.query.max || '3000', 10);
    const ids = [];
    let cursorId = null, pages = 0, lastStatus = null;
    while (ids.length < max && pages < 60) {
      const r = await backfillListPage(disc.field, disc.value, cursorId, 100);
      lastStatus = r.status;
      if (r.status >= 400) break;
      for (const it of r.items) { if (it && it.id) ids.push(it.id); }
      pages++;
      if (!r.next || r.items.length === 0) break;
      cursorId = backfillNextCursor(r.next);
    }
    const uniqueIds = Array.from(new Set(ids));
    return res.json({
      ok: true, field: disc.field, valueFormat: disc.fmt, value: disc.value,
      windowEmpty: !!disc.empty, lastStatus: lastStatus, pages: pages,
      count: uniqueIds.length, sample: uniqueIds.slice(0, 10), ids: uniqueIds
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

// ‚îÄ‚îÄ v7.5 BACKFILL SCAN (para el hueco COMPLETO, incl. clientes recurrentes) ‚îÄ‚îÄ
// La API publica solo deja filtrar contactos por createdAt (no por "ultimo mensaje").
// Para atrapar tambien a los recurrentes activos en la ventana, este endpoint pagina
// TODOS los contactos (por createdAt <= to) y para cada uno mira el timestamp de su
// ultimo mensaje; marca inWindow si cae en [from,to]. Lo maneja el backfill.py local
// (paginado + resumible). READ-ONLY: no escribe ni taggea.
//   GET /backfill/scan?token=..&from=YYYY-MM-DD&to=YYYY-MM-DD&cursorId=&limit=40
//   -> { results:[{id, ts, inWindow}], nextCursorId, scanned, sampleMsgKeys, sampleMsg }

// extrae el timestamp (epoch ms) del objeto mensaje. OJO: los mensajes de Respond.io v2
// NO traen campo de fecha explicito; el timestamp va codificado en messageId (epoch en
// MICROSEGUNDOS, ej. 1784052692000000 -> /1000 = ms). Normaliza por magnitud por si acaso.
function backfillMsgTs(msg) {
  if (!msg) return null;
  const cands = [msg.messageId, msg.timestamp, msg.createdAt, msg.created_at, msg.time,
    msg.messageTime, msg.sentAt, msg.sent_at, msg.date];
  for (let v of cands) {
    if (v == null) continue;
    let n = null;
    if (typeof v === 'number') n = v;
    else if (typeof v === 'string' && /^\d+$/.test(v)) n = Number(v);
    else if (typeof v === 'string') { const t = Date.parse(v); if (!isNaN(t)) return t; continue; }
    if (n == null || !isFinite(n)) continue;
    if (n >= 1e15) return Math.floor(n / 1000); // microsegundos -> ms
    if (n >= 1e12) return n;                     // ya en ms
    if (n >= 1e9) return n * 1000;              // segundos -> ms
  }
  return null;
}

async function backfillFetchMessages(contactId, limit, attempt) {
  const r = await fetch('https://api.respond.io/v2/contact/id:' + contactId + '/message/list?limit=' + (limit || 3), {
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' }
  });
  if (r.status === 429 && (attempt || 0) < 2) {
    const ra = parseInt(r.headers.get('retry-after') || '2', 10);
    await new Promise(z => setTimeout(z, (ra || 2) * 1000));
    return backfillFetchMessages(contactId, limit, (attempt || 0) + 1);
  }
  if (!r.ok) return { status: r.status, messages: [] };
  const j = await r.json().catch(() => null);
  return { status: r.status, messages: (j && (j.data || j.items)) || [] };
}

app.get('/backfill/scan', async (req, res) => {
  if (!BACKFILL_TOKEN) return res.status(403).json({ error: 'backfill deshabilitado: setea BACKFILL_TOKEN' });
  if (req.query.token !== BACKFILL_TOKEN) return res.status(401).json({ error: 'token invalido' });
  const from = req.query.from, to = req.query.to;
  if (!from || !to) return res.status(400).json({ error: 'faltan from y to (YYYY-MM-DD)' });

  const limit = Math.min(parseInt(req.query.limit || '40', 10), 60);
  const cursorId = req.query.cursorId || null;
  const fromMs = new Date(from + 'T00:00:00-06:00').getTime();
  const toMs = new Date(to + 'T23:59:59-06:00').getTime();

  try {
    // 1) una pagina de contactos (todos los creados hasta 'to', datetime que ya sabemos que sirve)
    const listVal = { from: backfillFmtValue('2015-01-01', 'from', 'datetime'), to: backfillFmtValue(to, 'to', 'datetime') };
    const page = await backfillListPage('createdAt', listVal, cursorId, limit);
    if (page.status >= 400) return res.status(502).json({ error: 'list fallo', status: page.status, body: (page.text || '').slice(0, 300) });

    const results = [];
    let sampleMsg = null, sampleMsgKeys = null;
    for (let i = 0; i < page.items.length; i++) {
      const it = page.items[i];
      if (!it || !it.id) continue;
      const mr = await backfillFetchMessages(it.id, 3, 0);
      const newest = mr.messages && mr.messages[0] ? mr.messages[0] : null;
      if (i === 0 && newest) { sampleMsg = newest; sampleMsgKeys = Object.keys(newest); }
      const ts = backfillMsgTs(newest);
      const inWindow = ts != null && ts >= fromMs && ts <= toMs;
      results.push({ id: it.id, ts: ts, inWindow: inWindow, msgStatus: mr.status, mid: newest ? newest.messageId : null });
      await new Promise(z => setTimeout(z, 120)); // suave con el rate limit
    }
    return res.json({
      ok: true, scanned: page.items.length,
      nextCursorId: page.next ? backfillNextCursor(page.next) : null,
      matched: results.filter(r => r.inWindow).map(r => r.id),
      results: results,
      sampleMsgKeys: sampleMsgKeys, sampleMsg: sampleMsg
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});



// ============================================================
// v8.0 FILMORENT REWARDS (Pilar 6) — 22-jul-2026
// Proxy server-side de Booqable para el portal rewards.filmorent.com.
// La key de Booqable vive en env (BOOQABLE_API_KEY) — NUNCA en el frontend.
//
//   GET  /rewards/member?email=...  -> puntos, tier-data, historial (calculado
//                                      de Booqable EN VIVO, excluyendo lineas
//                                      ELSEPC) menos canjes del Ledger
//   POST /rewards/redeem            -> valida saldo y registra canje en Ledger
//   POST /rewards/scan              -> resuelve QR de miembro y registra scan
//   GET  /rewards/folio?f=RWD-...   -> staff: estado de un folio de canje (Ledger)
//   POST /rewards/folio/aplicar     -> staff: marca un folio como aplicado a orden
//
// Persistencia: Google Sheet "Rewards Ledger" via Apps Script (doPost para
// escribir canjes/scans, doGet para leer canjes por customer) — mismo patron
// logToGoogleSheets que el Log de Conversaciones. Env: REWARDS_SHEETS_URL.
//
// Reglas de negocio (pilar6-rewards/README.md):
//   - 1 pt / $100 MXN. Base = grand_total_in_cents (sin IVA) de ordenes con
//     status distinto de draft/concept/canceled — verificado que la suma
//     coincide EXACTO con customer.revenue_in_cents.
//   - La linea del producto ELSEPC no genera puntos (subrenta: 90% no es
//     ingreso nuestro). El precio de linea viene CON IVA, asi que se convierte
//     a base sin IVA con el ratio grand_total/grand_total_with_tax de su orden.
//   - Canje = "crédito calibrado" (decisión F0, 23-jul-2026): crédito en pesos
//     por nivel de puntos, con doble tope y dedupe. Ver rewardsCatalogFor().
//   - QR determinstico del customer_id, formato FLM-XX-YYYY-XXNX. Hash FNV-1a
//     32-bit (el hash del prototipo — suma de charCodes — solo producia ~30k
//     valores => colisiones casi seguras entre ~2k miembros).
// Deploys automatizados con token fine-grained desde 27-jul-2026.
// ============================================================

const BOOQABLE_API_KEY = process.env.BOOQABLE_API_KEY;
const BOOQABLE_BASE = process.env.BOOQABLE_BASE || 'https://filmorent-sa-de-cv.booqable.com/api/4';
const REWARDS_SHEETS_URL = process.env.REWARDS_SHEETS_URL; // Apps Script del Rewards Ledger
const REWARDS_STAFF_PIN = process.env.REWARDS_STAFF_PIN;   // PIN genérico (legacy/respaldo)
// v8.5: PIN individual por empleado — env REWARDS_STAFF_PINS con pares
// "1111:Suheidi,2222:Eduardo" (acepta pin:nombre o nombre:pin). Con esto el
// Ledger registra QUIÉN hizo cada scan/canje/aplicación con su propio PIN.
const REWARDS_STAFF_PINS = (() => {
  const map = new Map();
  String(process.env.REWARDS_STAFF_PINS || '').split(',').forEach(pair => {
    const p = pair.split(':').map(s => s.trim()).filter(Boolean);
    if (p.length !== 2) return;
    if (/^\d{3,10}$/.test(p[0])) map.set(p[0], p[1]);
    else if (/^\d{3,10}$/.test(p[1])) map.set(p[1], p[0]);
  });
  return map;
})();
// (La antigua rewardsStaffFromPin() devolvía '' cuando no había PINs en el env
//  = acceso libre al mostrador. Se ELIMINÓ en la auditoría del 25-jul-2026:
//  la validación del PIN vive ahora dentro de rewardsStaffFrom(), fail-closed.)

// ── v8.6: LOGIN DE STAFF CON GOOGLE WORKSPACE ────────────────
// Decisión de Daniel 25-jul-2026: el mostrador se identifica con la cuenta
// corporativa (@filmorent.com), no con un PIN compartido. Al dar de baja a
// alguien en Workspace pierde el acceso solo; el Ledger registra su email real.
// El navegador manda el ID token de Google Identity Services; aquí se VERIFICA
// contra Google (endpoint oficial tokeninfo — evita meter dependencias nuevas
// al package.json de producción) y se exige: firma válida, aud == nuestro
// client id, no expirado, email verificado y dominio corporativo.
const REWARDS_GOOGLE_CLIENT_ID = process.env.REWARDS_GOOGLE_CLIENT_ID || '';
// Ventana para que el propio cliente reclame una renta desde su portal (self-service).
// En mostrador no aplica: ahí lo registra un empleado autenticado.
const REWARDS_ATRIB_DIAS = parseInt(process.env.REWARDS_ATRIB_DIAS || '14', 10);
const REWARDS_STAFF_DOMAIN = (process.env.REWARDS_STAFF_DOMAIN || 'filmorent.com').toLowerCase();
// Allowlist opcional: si trae emails, SOLO esos entran (aunque sean del dominio).
const REWARDS_STAFF_EMAILS = new Set(
  String(process.env.REWARDS_STAFF_EMAILS || '')
    .split(',').map(s => s.trim().toLowerCase()).filter(Boolean)
);
// Cache de tokens ya verificados (clave = token, valor = {name, exp}). Evita
// pegarle a Google en cada request del mostrador; caduca con el propio token.
const rewardsTokenCache = new Map();

async function rewardsStaffFromGoogle(idToken) {
  const tok = String(idToken || '').trim();
  if (!tok) return null;
  const now = Math.floor(Date.now() / 1000);
  const hit = rewardsTokenCache.get(tok);
  if (hit && hit.exp > now + 30) return hit.name;
  try {
    const r = await fetch('https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(tok));
    if (!r.ok) return null;
    const t = await r.json();
    // aud: el token DEBE haber sido emitido para nuestro client id
    if (!REWARDS_GOOGLE_CLIENT_ID || t.aud !== REWARDS_GOOGLE_CLIENT_ID) {
      console.error('[rewards] token de Google con aud incorrecto');
      return null;
    }
    if (String(t.iss || '').indexOf('accounts.google.com') === -1) return null;
    const exp = parseInt(t.exp, 10) || 0;
    if (!exp || exp <= now) return null;
    if (String(t.email_verified) !== 'true') return null;
    const email = String(t.email || '').toLowerCase();
    // 'hd' (hosted domain) SOLO lo emite Google para cuentas Workspace del
    // dominio; el sufijo del email no basta (auditoría 25-jul). Se exige hd
    // Y que el email termine en el mismo dominio.
    const domain = String(t.hd || '').toLowerCase();
    if (domain !== REWARDS_STAFF_DOMAIN || email.slice(-(REWARDS_STAFF_DOMAIN.length + 1)) !== ('@' + REWARDS_STAFF_DOMAIN)) {
      console.error('[rewards] login de staff rechazado (dominio ' + domain + ')');
      return null;
    }
    if (REWARDS_STAFF_EMAILS.size > 0 && !REWARDS_STAFF_EMAILS.has(email)) {
      console.error('[rewards] login de staff fuera de la lista: ' + email);
      return null;
    }
    const name = String(t.name || '').trim() || email;
    if (rewardsTokenCache.size > 200) rewardsTokenCache.clear();
    rewardsTokenCache.set(tok, { name: name, exp: exp });
    return name;
  } catch (e) {
    console.error('[rewards] error verificando token de Google: ' + e.message);
    return null;
  }
}

// ── FAIL-CLOSED (auditoría 25-jul-2026) ──────────────────────
// Antes, "sin PINs configurados" significaba ACCESO LIBRE: al retirar el PIN
// para dejar solo Google, los 4 endpoints de mostrador (incluido /pagar, que
// mueve dinero real en Booqable) quedaban abiertos a internet. Ahora:
// sin credencial válida NO se pasa, y si NO hay ninguna forma de auth
// configurada el mostrador responde 503 (deshabilitado), nunca "pásale".
const REWARDS_STAFF_PROTECTED = !!(REWARDS_GOOGLE_CLIENT_ID || REWARDS_STAFF_PIN || REWARDS_STAFF_PINS.size > 0);
// Escotilla explícita SOLO para desarrollo local: se ignora en Render aunque
// alguien la ponga por error en el env de producción.
const REWARDS_STAFF_OPEN = process.env.REWARDS_STAFF_OPEN === '1' &&
  !process.env.RENDER && process.env.NODE_ENV !== 'production';
// Config rota (env de PINs escrita pero ninguna pareja válida) => tratar como
// mal configurado y bloquear, no como "sin protección".
const REWARDS_STAFF_PINS_BROKEN = !!String(process.env.REWARDS_STAFF_PINS || '').trim() && REWARDS_STAFF_PINS.size === 0;

// ── ANTI-FUERZA-BRUTA DEL PIN ────────────────────────────────
// La IP del cliente NO es confiable (cualquier header se puede falsificar y el
// portal pega directo al origin de Render), así que el candado REAL no puede
// depender de ella. Diseño (2ª auditoría 25-jul-2026):
//   · Solo cuentan los intentos que TRAEN un PIN — un anónimo no puede
//     provocar el bloqueo del equipo (evita el DoS).
//   · Tope GLOBAL de intentos de PIN por ventana, con backoff exponencial:
//     cada vez que se agota, el PIN queda deshabilitado el doble de tiempo
//     (15 min → 30 → 60 → … hasta 8 h). El login de Google NUNCA se bloquea,
//     así que el mostrador puede seguir operando durante un ataque.
//   · El contador por IP se conserva solo como señal para los logs.
const rewardsAuthFails = new Map();
const AUTH_WINDOW_MS = 15 * 60 * 1000;
const PIN_FAILS_MAX = 25;                 // intentos de PIN por ventana
let rewardsPinGate = { start: 0, n: 0, strikes: 0, blockedUntil: 0 };

function rewardsPinDisabled() {
  return Date.now() < rewardsPinGate.blockedUntil;
}
// Registra un intento FALLIDO que sí traía credencial de PIN.
function rewardsPinFail(ip) {
  const now = Date.now();
  if (now - rewardsPinGate.start > AUTH_WINDOW_MS) {
    rewardsPinGate.start = now;
    rewardsPinGate.n = 0;
    if (now - rewardsPinGate.blockedUntil > 6 * 3600 * 1000) rewardsPinGate.strikes = 0; // se calma solo
  }
  rewardsPinGate.n++;
  let b = rewardsAuthFails.get(ip);
  if (!b || now - b.start > AUTH_WINDOW_MS) { b = { start: now, n: 0 }; rewardsAuthFails.set(ip, b); }
  b.n++;
  if (rewardsAuthFails.size > 5000) rewardsAuthFails.clear();
  if (rewardsPinGate.n >= PIN_FAILS_MAX) {
    rewardsPinGate.strikes = Math.min(rewardsPinGate.strikes + 1, 5);
    const castigo = AUTH_WINDOW_MS * Math.pow(2, rewardsPinGate.strikes - 1); // 15m,30m,1h,2h,4h
    rewardsPinGate.blockedUntil = now + castigo;
    rewardsPinGate.n = 0;
    console.error('[rewards] PIN de staff DESHABILITADO ' + Math.round(castigo / 60000) +
      ' min por exceso de intentos fallidos (ultimo desde ' + ip + '). El login de Google sigue activo.');
  } else {
    console.error('[rewards] intento de PIN fallido desde ' + ip +
      ' (' + rewardsPinGate.n + '/' + PIN_FAILS_MAX + ' en la ventana global)');
  }
}

// Máximo 3 canjes por miembro cada 24 h (el portal del cliente no tiene login).
const rewardsRedeemsPorMiembro = new Map();
function rewardsRedeemAllowed(customerId) {
  const now = Date.now();
  let b = rewardsRedeemsPorMiembro.get(customerId);
  if (!b || now - b.start > 24 * 3600 * 1000) { b = { start: now, n: 0 }; rewardsRedeemsPorMiembro.set(customerId, b); }
  if (b.n >= 3) return false;
  b.n++;
  if (rewardsRedeemsPorMiembro.size > 5000) rewardsRedeemsPorMiembro.clear();
  return true;
}

// Comparación de secretos en tiempo constante (evita filtrar el PIN por timing)
function rewardsSecretEq(a, b) {
  const x = String(a || ''), y = String(b || '');
  let d = x.length ^ y.length;
  const n = Math.max(x.length, y.length);
  for (let i = 0; i < n; i++) d |= (x.charCodeAt(i) || 0) ^ (y.charCodeAt(i) || 0);
  return d === 0;
}

// Credenciales de una petición: header Authorization (preferido — no queda en
// logs ni en el referer), luego body, luego query (solo para el PIN).
function rewardsAuthSrc(req) {
  const h = String(req.headers.authorization || '');
  const bearer = /^Bearer\s+(.+)$/i.exec(h);
  const b = req.body || {};
  const q = req.query || {};
  return {
    id_token: (bearer && bearer[1]) || b.id_token || q.id_token || '',
    pin: b.pin || q.pin || ''
  };
}

// Identifica al staff de una petición: primero Google Workspace, si no hay
// token cae al PIN (respaldo). Devuelve el nombre a registrar en el Ledger,
// o null si no hay credencial válida.
async function rewardsStaffFrom(req) {
  const s = rewardsAuthSrc(req);
  const ip = rewardsClientIp(req);
  const tokenName = await rewardsStaffFromGoogle(s.id_token);
  if (tokenName) return tokenName;
  const pin = String(s.pin || '').trim();
  if (pin && !rewardsPinDisabled() && !REWARDS_STAFF_PINS_BROKEN) {
    // comparación de tiempo constante (no filtra el PIN por timing)
    if (REWARDS_STAFF_PINS.size > 0) {
      for (const [k, nombre] of REWARDS_STAFF_PINS) if (rewardsSecretEq(pin, k)) return nombre;
    }
    if (REWARDS_STAFF_PIN && rewardsSecretEq(pin, REWARDS_STAFF_PIN)) return 'staff';
  }
  // desarrollo local sin ninguna auth configurada
  if (!REWARDS_STAFF_PROTECTED && REWARDS_STAFF_OPEN) return 'dev';
  // SOLO cuentan los fallos que traían credencial: un anónimo no puede
  // provocar el bloqueo del PIN de todo el equipo
  if (pin) rewardsPinFail(ip);
  return null;
}

// Guard de los endpoints de mostrador. true = ya respondió (denegado).
function rewardsStaffDenied(res, staffName) {
  if (staffName) return false;
  if (!REWARDS_STAFF_PROTECTED) {
    res.status(503).json({ ok: false, error: 'mostrador deshabilitado: falta configurar el acceso de staff' });
    return true;
  }
  res.status(401).json({ ok: false, error: 'inicia sesion con tu cuenta @' + REWARDS_STAFF_DOMAIN + ' (o usa tu PIN)' });
  return true;
}

// LEGACY — Catalogo v1 (solo % de descuento). Ya NO lo usa nadie: desde la
// decision F0 (crédito calibrado, 23-jul-2026) el catálogo se personaliza por
// miembro con rewardsCatalogFor(). Se conserva solo como documentación/fallback.
const REWARDS_CATALOG = [
  { id: 1, name: '5% descuento en tu próxima renta', points: 100, value: 5 },
  { id: 2, name: '10% descuento en tu próxima renta', points: 250, value: 10 },
  { id: 3, name: '15% descuento en tu próxima renta', points: 500, value: 15 },
  { id: 4, name: '20% descuento en tu próxima renta', points: 800, value: 20 },
  { id: 5, name: '25% descuento en tu próxima renta', points: 1200, value: 25 }
];

// ── Crédito calibrado (decisión F0 de Daniel, 23-jul-2026) ──
// El premio deja de ser % y pasa a ser CRÉDITO EN PESOS con doble tope:
//   crédito del nivel = MIN(techo del nivel, 50% del ticket promedio del
//   miembro redondeado a múltiplos de $50), con piso de $100.
// Ids estables por nivel (el Ledger los referencia): 50→6 (escalón de entrada,
// nuevo), 100→1, 250→2, 500→3, 800→4, 1200→5. Reglas de mostrador que NO
// cambian (solo copy): 1 canje por orden, el folio no da cambio, la orden debe
// ser ≥ 2× el crédito.
const REWARDS_CREDIT_LEVELS = [
  { id: 6, points: 50,   cap_cents: 10000 },   // $100
  { id: 1, points: 100,  cap_cents: 25000 },   // $250
  { id: 2, points: 250,  cap_cents: 70000 },   // $700
  { id: 3, points: 500,  cap_cents: 160000 },  // $1,600
  { id: 4, points: 800,  cap_cents: 280000 },  // $2,800
  { id: 5, points: 1200, cap_cents: 450000 }   // $4,500
];

// "$1,600" estilo es-MX sin centavos (manual: no depende de ICU en el runtime).
function rewardsFormatMXN(cents) {
  const pesos = Math.round((cents || 0) / 100);
  return '$' + String(pesos).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

// Catálogo personalizado del miembro. DEDUPE: si el tope del ticket hace que
// dos niveles den el mismo crédito, solo se ofrece el de MENOS puntos — cada
// nivel listado debe dar un crédito estrictamente mayor que el anterior.
function rewardsCatalogFor(avgTicketCents) {
  // 50% del ticket promedio, redondeado a múltiplos de $50 (5000 centavos)
  const halfTicket = Math.round((avgTicketCents || 0) / 2 / 5000) * 5000;
  const catalog = [];
  let prevCredit = 0;
  for (const lvl of REWARDS_CREDIT_LEVELS) {
    const credit = Math.max(10000, Math.min(lvl.cap_cents, halfTicket)); // piso $100
    if (credit <= prevCredit) continue;
    prevCredit = credit;
    catalog.push({
      id: lvl.id,
      // 26-ago-2026 (Daniel): nunca "crédito" de cara al cliente — suena a deuda
      name: 'Descuento de ' + rewardsFormatMXN(credit) + ' en tu próxima renta',
      points: lvl.points,
      credito_cents: credit
    });
  }
  return catalog;
}

// Nivel por RENTAS DE LOS ÚLTIMOS 12 MESES (decisión de Daniel 24-jul-2026):
// el estatus se gana y se defiende cada año; los puntos canjeables NO caducan.
// min en centavos SIN IVA de revenue rodante de 12 meses (base ya sin
// exclusiones). Censo 24-jul: Plata $20k+/año = 57 clientes (top 5% de 1,114
// activos), Oro $100k+/año = 5 clientes.
// Rediseño 1-ago-2026 (decisión de Daniel con pronóstico): el beneficio de nivel
// es MULTIPLICADOR de puntos, no descuento % (el 5/10% costaba ~$188k/año
// incondicionales; el multiplicador ~$32k condicionados a que el cliente regrese).
// Regla DUAL de calificación (tipo aerolínea): por pesos O por # de rentas.
// discount queda en 0 por compatibilidad con el portal (ya no existe descuento
// permanente ni la regla "aplica el mayor").
const REWARDS_TIERS = [
  { name: 'Bronce', min_12m_cents: 0, min_rentas_12m: null, mult: 1, discount: 0 },
  { name: 'Plata', min_12m_cents: 2000000, min_rentas_12m: 12, mult: 1.5, discount: 0 },   // $20k O 12+ rentas
  { name: 'Oro', min_12m_cents: 10000000, min_rentas_12m: 24, oro_piso_cents: 5000000, mult: 2, discount: 0 } // $100k O (24+ rentas Y $50k)
];
// Renta mínima para CONTAR en la vía de frecuencia: $500 sin IVA con 12+ rentas
// (decisión de Daniel 1-ago-2026: "renta una vez al mes y eres Plata" — grupo más
// puro, 8.5 meses activos promedio; $750/8 daba 11 clientes, $500/12 da 6).
// Además 1 renta por rango de fechas.
const REWARDS_RENTA_MIN_CENTS = 50000;
// El multiplicador aplica SOLO a rentas desde el lanzamiento; lo retroactivo va a 1x
// (no regalarle miles de puntos de golpe a los Oro).
const REWARDS_MULT_DESDE = '2026-08-01';

// Vigencia por ACTIVIDAD (decisión de Daniel 6-ago-2026, estilo Club Comex):
// los puntos viven mientras la cuenta rente al menos una vez cada 6 meses.
// Un hueco de más de 183 días sin rentas (contado solo desde el arranque del
// programa) CADUCA lo acumulado hasta ese hueco — definitivo: las rentas
// posteriores acumulan desde cero. El reloj de los puntos de arranque corre
// desde REWARDS_ARRANQUE, no desde la última renta vieja (si no, los dormidos
// del win-back nacerían caducados). Términos §6 v2.2.
const REWARDS_INACTIVIDAD_DIAS = 183;
const REWARDS_ARRANQUE = '2026-08-06';

// Recorre las fechas de renta (ascendentes, ISO) y devuelve:
//  · vivo_desde: solo órdenes con fecha >= vivo_desde generan puntos (null = todas)
//  · caducado:   el saldo actual ya venció (hueco de 183d hasta hoy)
//  · limite:     fecha en que caduca el saldo si no vuelve a rentar
// El hueco histórico PERSISTE en la serie de fechas, así que un caducado que
// vuelve a rentar acumula desde cero sin revivir lo viejo.
function rewardsVigencia(fechasAsc, ahoraMs) {
  const GAP = REWARDS_INACTIVIDAD_DIAS * 86400000;
  const arranque = new Date(REWARDS_ARRANQUE + 'T00:00:00Z').getTime();
  let vivoDesde = null;
  let reloj = arranque;
  for (const f of fechasAsc) {
    const t = new Date(f).getTime();
    if (isNaN(t)) continue;
    // Solo las rentas YA OCURRIDAS mueven el reloj. Sin este filtro, una reserva
    // agendada a más de 183 días se leía como "hueco de inactividad" y ponía
    // vivo_desde en el FUTURO, borrando todos los puntos ya ganados del cliente
    // (auditoría 19-ago-2026: nadie lo había sufrido aún, pero era cuestión de
    // que alguien reservara un rodaje lejano).
    if (t > ahoraMs) continue;
    if (t > reloj) {
      if (t - reloj > GAP) vivoDesde = f;   // hubo hueco: lo anterior caduca
      reloj = t;
    }
  }
  const caducado = (ahoraMs - reloj) > GAP;
  return {
    vivo_desde: caducado ? '9999-12-31' : vivoDesde,
    caducado: caducado,
    limite: new Date(reloj + GAP).toISOString().slice(0, 10)
  };
}

// Cuentas que NO participan en el programa (decisión de Daniel 24-jul-2026):
// socios/internos. No aparecen en /member, /redeem, /pagar ni en el índice QR.
const REWARDS_EXCLUDED_CUSTOMER_IDS = new Set([
  '4263eb3e-1448-47a6-974d-73494fe8783b'  // Pasumecha Producciones (socio)
]);
// Órdenes que no acumulan puntos: ventas de equipo capturadas como orden.
// El programa premia renta recurrente; una compra de equipo es transacción
// única (reversible: quitar el número de esta lista si Daniel decide que
// las ventas sí acumulen). Ventas futuras: capturar la línea empezando con
// "VENTA" y queda excluida sola (regla en rewardsLineExcluded).
const REWARDS_EXCLUDED_ORDER_NUMBERS = new Set([
  10261  // Blackbear 8-jul-2026: venta Ronin 2 + Ready Rig GS ProArm ($130k)
]);

// CORS solo para /rewards/* (el portal vive en otro dominio).
const REWARDS_ORIGINS = [
  'https://rewards.filmorent.com',
  'https://rewards-filmorent.daniel-85f.workers.dev',
  'https://filmorent.com',
  'https://www.filmorent.com'
];
// Rate limit simple por IP (el endpoint es publico y devuelve datos de miembro):
// 30 requests por ventana de 5 min. En memoria — suficiente para un solo dyno.
const rewardsRate = new Map();
// IP aproximada del cliente, para el rate limit y el audit trail del Ledger.
// ⚠️ NO ES CONFIABLE para seguridad: el portal pega directo al origin de Render
// y cualquiera puede mandar los headers que quiera (2ª auditoría 25-jul-2026).
// Por eso el candado del PIN es un tope GLOBAL con backoff, no un bloqueo por
// IP. Aquí se prefiere el primer x-forwarded-for porque es el que trae la IP
// pública real del cliente en el tráfico legítimo (verificado en el Ledger).
function rewardsClientIp(req) {
  const xff = String(req.headers['x-forwarded-for'] || '').split(',').map(s => s.trim()).filter(Boolean);
  if (xff.length) return xff[0].slice(0, 60);
  const cf = String(req.headers['cf-connecting-ip'] || '').trim();
  if (cf) return cf.slice(0, 60);
  return String(req.ip || 'desconocida').slice(0, 60);
}
function rewardsClientUa(req) {
  return String(req.headers['user-agent'] || '').slice(0, 150);
}
function rewardsRateOk(ip) {
  const now = Date.now();
  let b = rewardsRate.get(ip);
  if (!b || now - b.start > 5 * 60 * 1000) { b = { start: now, n: 0 }; rewardsRate.set(ip, b); }
  b.n++;
  if (rewardsRate.size > 5000) rewardsRate.clear(); // tope de memoria
  return b.n <= 30;
}

app.use('/rewards', (req, res, next) => {
  const origin = req.headers.origin || '';
  if (REWARDS_ORIGINS.includes(origin) || /^http:\/\/localhost(:\d+)?$/.test(origin) || origin === 'null') {
    res.setHeader('Access-Control-Allow-Origin', origin);
  }
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  // 'Authorization' habilita mandar el ID token en header (fuera de la URL)
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (!BOOQABLE_API_KEY) return res.status(503).json({ ok: false, error: 'rewards deshabilitado: falta BOOQABLE_API_KEY en el env' });
  if (!rewardsRateOk(rewardsClientIp(req))) return res.status(429).json({ ok: false, error: 'demasiadas solicitudes, espera unos minutos' });
  next();
});

// GET a Booqable con retry simple en 429.
async function booqableGet(pathWithQuery, attempt) {
  const r = await fetch(BOOQABLE_BASE + pathWithQuery, {
    headers: { 'Authorization': 'Bearer ' + BOOQABLE_API_KEY, 'Accept': 'application/json' }
  });
  if (r.status === 429 && (attempt || 0) < 2) {
    const ra = parseInt(r.headers.get('retry-after') || '2', 10);
    await new Promise(z => setTimeout(z, (ra || 2) * 1000));
    return booqableGet(pathWithQuery, (attempt || 0) + 1);
  }
  if (!r.ok) {
    const body = await r.text().catch(() => '');
    const err = new Error('Booqable ' + r.status + ': ' + body.slice(0, 200));
    err.status = r.status;
    throw err;
  }
  return r.json();
}

// Escritura a Booqable (POST/PATCH/DELETE) con el mismo retry en 429.
// Verificado 24-jul-2026 en orden de prueba: POST /lines con owner_id/owner_type
// crea la línea y los totales de la orden se recalculan solos.
async function booqableWrite(method, path, payload, attempt) {
  const r = await fetch(BOOQABLE_BASE + path, {
    method: method,
    headers: {
      'Authorization': 'Bearer ' + BOOQABLE_API_KEY,
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: payload === undefined ? undefined : JSON.stringify(payload)
  });
  if (r.status === 429 && (attempt || 0) < 2) {
    const ra = parseInt(r.headers.get('retry-after') || '2', 10);
    await new Promise(z => setTimeout(z, (ra || 2) * 1000));
    return booqableWrite(method, path, payload, (attempt || 0) + 1);
  }
  if (!r.ok) {
    const body = await r.text().catch(() => '');
    const err = new Error('Booqable ' + r.status + ': ' + body.slice(0, 200));
    err.status = r.status;
    throw err;
  }
  return r.status === 204 ? null : r.json();
}

// QR de miembro: FLM-XX-YYYY-XXNX, deterministico del customer_id (FNV-1a 32-bit).
function rewardsQrCode(customerId) {
  let h = 0x811c9dc5;
  for (let i = 0; i < customerId.length; i++) {
    h ^= customerId.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  const L = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; // sin I/O (se confunden con 1/0)
  const a = L[h % 24];
  const b = L[Math.floor(h / 24) % 24];
  const year = 2025 + (Math.floor(h / 576) % 2);
  const c = L[Math.floor(h / 1152) % 24];
  const n = Math.floor(h / 27648) % 10;
  const d = L[Math.floor(h / 276480) % 24];
  const e = Math.floor(h / 6635520) % 9;
  return 'FLM-' + a + b + '-' + year + '-' + c + n + d + e;
}

// Regla dual: pesos O rentas. Para Oro la vía de frecuencia exige además un piso
// de pesos ($50k) — que nadie llegue a Oro rentando 30 veces tarjetas SD.
function rewardsTierFor(revenue12mCents, rentas12m) {
  const r = rentas12m || 0;
  const oro = REWARDS_TIERS[2], plata = REWARDS_TIERS[1];
  if (revenue12mCents >= oro.min_12m_cents ||
      (r >= oro.min_rentas_12m && revenue12mCents >= oro.oro_piso_cents)) return oro;
  if (revenue12mCents >= plata.min_12m_cents || r >= plata.min_rentas_12m) return plata;
  return REWARDS_TIERS[0];
}

// Busca el customer por email exacto en Booqable. null si no existe.
async function rewardsFindCustomer(email) {
  const d = await booqableGet('/customers?filter[email]=' + encodeURIComponent(email) + '&page[size]=1');
  const c = (d.data || [])[0];
  return c || null;
}

// Lineas que NO generan puntos (regla de Daniel 22-jul-2026): subrenta ELSEPC
// (90% no es ingreso nuestro) + cargos de servicio que no son renta de equipo
// (pintura del estudio, personal: staff/encargados/operadores/gaffer). El equipo
// del bono del estudio NO aparece como linea cobrada (va dentro del precio del
// paquete), asi que no requiere exclusion. Nombres verificados contra el catalogo.
const REWARDS_EXCLUDE_SUBSTR = [
  'elsepc',                    // ELSEPC PureBB trifasico (subrenta)
  'pintura y regreso',         // Pintura y regreso a estado original
  'hora extra personal',
  'operador prompter',
  'encargado de estudio',      // todas las variantes (Alfredo/Barush/... y "ya no usar")
  'gaffer'                     // por si se captura personal como linea libre
];
function rewardsLineExcluded(title) {
  const t = (title || '').toLowerCase().trim();
  if (!t) return false;
  if (REWARDS_EXCLUDE_SUBSTR.some(k => t.indexOf(k) !== -1)) return true;
  // producto "Staff" (personal) — match estricto para no rozar nombres de equipo
  if (t === 'staff' || t.indexOf('staff ') === 0 || t.indexOf('staff -') === 0) return true;
  // ventas de equipo capturadas como línea libre ("VENTA - Ronin 2...") —
  // prefijo estricto para no rozar títulos que contengan "ventana" etc.
  if (t.indexOf('venta ') === 0 || t.indexOf('venta-') === 0 || t.indexOf('venta:') === 0) return true;
  return false;
}

// Calcula puntos ganados + historial de un customer, excluyendo las lineas de
// REWARDS_EXCLUDE_SUBSTR. Verificado: sum(grand_total sin draft) === revenue_in_cents.
async function rewardsComputeEarned(customerId) {
  // 1) todas las ordenes del cliente
  const orders = [];
  for (let page = 1; page <= 10; page++) {
    const od = await booqableGet('/orders?filter[customer_id]=' + customerId +
      '&sort=-created_at&page[size]=100&page[number]=' + page);
    const data = od.data || [];
    orders.push(...data);
    if (data.length < 100) break;
  }
  const countable = orders.filter(o => {
    const a = o.attributes || {};
    if (a.status === 'draft' || a.status === 'concept' || a.status === 'canceled') return false;
    if (REWARDS_EXCLUDED_ORDER_NUMBERS.has(a.number)) return false; // ventas de equipo
    return true;
  });

  // 2) lineas ELSEPC de esas ordenes, en lotes de 25 order_ids
  const elsepcByOrder = {};
  const ids = countable.map(o => o.id);
  for (let i = 0; i < ids.length; i += 25) {
    const batch = ids.slice(i, i + 25).join(',');
    for (let lpage = 1; lpage <= 10; lpage++) {
      const ld = await booqableGet('/lines?filter[order_id]=' + batch +
        '&page[size]=100&page[number]=' + lpage);
      const data = ld.data || [];
      for (const l of data) {
        const la = l.attributes || {};
        if (la.archived) continue;
        if (!rewardsLineExcluded(la.title)) continue;
        elsepcByOrder[la.order_id] = (elsepcByOrder[la.order_id] || 0) + (la.price_in_cents || 0);
      }
      if (data.length < 100) break;
    }
  }

  // 3) base de puntos por orden = grand_total - ELSEPC (convertido a base sin IVA)
  let totalBaseCents = 0;
  let totalElsepcCents = 0;
  // revenue rodante de 12 meses (base del NIVEL): fecha de la renta
  // (starts_at) o de creación si no la hay
  const cutoff12m = new Date(Date.now() - 365 * 24 * 3600 * 1000).toISOString();
  let revenue12mCents = 0;
  const pre = countable.map(o => {
    const a = o.attributes || {};
    const g = a.grand_total_in_cents || 0;
    const gt = a.grand_total_with_tax_in_cents || 0;
    const elWithTax = elsepcByOrder[o.id] || 0;
    const ratio = gt ? (g / gt) : 1;
    const elBase = Math.min(Math.round(elWithTax * ratio), g);
    return { o, a, g, gt, elBase, base: Math.max(0, g - elBase),
      fecha: String(a.starts_at || a.created_at || '') };
  });

  // Multiplicador por orden = nivel VIGENTE AL MOMENTO de esa renta (ventana de
  // 12 meses ANTES de la renta, regla dual). Solo rentas desde REWARDS_MULT_DESDE;
  // lo anterior (incluido lo retroactivo del lanzamiento) queda a 1x. Así los
  // puntos ya ganados nunca se devalúan si el cliente baja de nivel.
  const rangoDe = a2 => String(a2.starts_at || '').slice(0, 10) + '|' + String(a2.stops_at || '').slice(0, 10);
  const asc = pre.slice().sort((x, y) => (x.fecha < y.fecha ? -1 : 1));
  const multByOrderId = {};
  for (let i = 0; i < asc.length; i++) {
    const r = asc[i];
    if (r.fecha.slice(0, 10) < REWARDS_MULT_DESDE) { multByOrderId[r.o.id] = 1; continue; }
    const desde = new Date(new Date(r.fecha).getTime() - 365 * 24 * 3600 * 1000).toISOString();
    let rev = 0;
    const rangosPrevios = new Set();
    for (let j = 0; j < i; j++) {
      const p = asc[j];
      if (p.fecha < desde) continue;
      rev += p.base;
      if (p.base >= REWARDS_RENTA_MIN_CENTS) rangosPrevios.add(rangoDe(p.a));
    }
    multByOrderId[r.o.id] = rewardsTierFor(rev, rangosPrevios.size).mult;
  }

  // rentas contables de 12m para el NIVEL actual (vía frecuencia): base >= $500
  // y UNA por rango de fechas — partir una orden en tres no cuenta triple.
  const rangos12m = new Set();
  let pointsBaseMultCents = 0;
  // Vigencia por actividad (§6): huecos de 6 meses caducan lo anterior. El
  // NIVEL no se toca aquí — se calcula solo con la ventana rodante de 12m.
  const fechasAsc = pre.map(r => r.fecha).sort();
  const vigencia = rewardsVigencia(fechasAsc, Date.now());
  const orderRows = pre.map(r => {
    totalBaseCents += r.g;
    totalElsepcCents += r.elBase;
    const mult = multByOrderId[r.o.id] || 1;
    if (!vigencia.vivo_desde || r.fecha >= vigencia.vivo_desde) {
      pointsBaseMultCents += Math.round(r.base * mult);
    }
    if (r.fecha >= cutoff12m) {
      revenue12mCents += r.base;
      if (r.base >= REWARDS_RENTA_MIN_CENTS) rangos12m.add(rangoDe(r.a));
    }
    return {
      id: r.o.id,
      number: r.a.number,
      status: r.a.status,
      total_cents: r.g,
      total_with_tax_cents: r.gt,
      elsepc_excluded_cents: r.elBase,
      points: Math.floor((r.base / 100 / 100) * mult),
      mult: mult,
      item_count: r.a.item_count || 0,
      payment_status: r.a.payment_status || '',
      starts_at: r.a.starts_at,
      stops_at: r.a.stops_at,
      created_at: r.a.created_at
    };
  });

  return {
    orders: orderRows,
    revenue_cents: totalBaseCents,
    revenue_12m_cents: revenue12mCents,
    rentas_12m: rangos12m.size,
    elsepc_excluded_cents: totalElsepcCents,
    vigencia: vigencia,   // {vivo_desde, caducado, limite} — §6 v2.2
    points_earned: Math.floor(pointsBaseMultCents / 100 / 100)
  };
}

// Lee del Ledger (Apps Script doGet) los canjes de un customer.
// Devuelve null si el Ledger no esta configurado o fallo (degradar con flag).
async function rewardsLedgerSummary(customerId, email, telefono) {
  if (!REWARDS_SHEETS_URL) return null;
  try {
    // v8.36: telefono (10 dígitos) también identifica cupones — un lead sin
    // cuenta en Booqable puede traer crédito promocional amarrado a su celular.
    const tel10 = rewardsPhone10(telefono);
    // Las consultas SIN customer_id exigen la clave en el Ledger v12 (un celular
    // se adivina; un UUID no) — se manda solo en ese caso para no regar la clave
    // en URLs donde no hace falta.
    const kParam = (!customerId && process.env.REWARDS_HITOS_KEY)
      ? '&k=' + encodeURIComponent(process.env.REWARDS_HITOS_KEY) : '';
    const r = await fetch(REWARDS_SHEETS_URL + '?action=member&customer_id=' + encodeURIComponent(customerId || '') +
      (email ? '&email=' + encodeURIComponent(String(email).toLowerCase()) : '') +
      (tel10 ? '&telefono=' + encodeURIComponent(tel10) : '') + kParam, { redirect: 'follow' });
    if (!r.ok) return null;
    const j = await r.json().catch(() => null);
    if (!j || j.ok === false) return null;
    return {
      exclusion: j.exclusion || null,   // tab Exclusiones: {nivel:'bloqueado'|'adeudo', motivo}
      cupones: j.cupones || [],         // crédito promocional en pesos (tab Cupones)
      redeemed_points: j.redeemed_points || 0,
      redemptions: j.redemptions || [],
      // 28-jul: órdenes de otros que este miembro pidió (suman) y órdenes propias
      // cuyos puntos cedió a quien las pidió (restan)
      atribuciones_ganadas: j.atribuciones_ganadas || [],
      atribuciones_cedidas: j.atribuciones_cedidas || []
    };
  } catch (e) {
    console.error('rewards ledger read error: ' + e.message);
    return null;
  }
}

// ── Candado de adeudos y vetos (términos §1 y §8, decisión de Daniel 5-ago-2026) ──
// Dos fuentes: la tab Exclusiones del Ledger (humana: la mantiene el equipo/Suheidi)
// y el proxy de Booqable (automática: orden TERMINADA hace más de 30 días con pago
// pendiente — medido 5-ago: 97-98% de las stopped están marcadas 'paid', así que
// las excepciones son adeudos reales, no basura de captura). La cancelación
// definitiva a 60 días NO es automática: la confirma Suheidi (dato irreversible).
// Devuelve null si puede canjear, o {status, error} para responder el 409.
// Fecha de HOY en Monterrey (UTC-6) como 'YYYY-MM-DD' — para vencer cupones
// con el mismo criterio de día que usa el contador de visitas.
function rewardsHoyMty() {
  return new Date(Date.now() - 6 * 3600 * 1000).toISOString().slice(0, 10);
}

// De la lista cruda de cupones del Ledger deja SOLO los usables hoy: estado
// 'activo' y (sin vencimiento O vence >= hoy MTY). El server manda estos al
// portal y valida contra ellos al aplicar — nunca confía en el vencimiento del
// cliente. Ordena por monto desc (el más grande primero).
function rewardsCuponesVigentes(cupones) {
  const hoy = rewardsHoyMty();
  // Normaliza el vencimiento a 'YYYY-MM-DD'. El Apps Script ya lo formatea así,
  // pero si Sheets coacciona la celda a Date y llega "Wed Sep 30 2026…", el
  // slice(0,10) da "Wed Sep 30" — que es < hoy alfabéticamente → se trata como
  // VENCIDO (fail-closed, nunca eterno). Defensa del hallazgo del 19-ago.
  const venceNorm = (v) => (v ? String(v).slice(0, 10) : null);
  return (cupones || [])
    .filter(c => c && String(c.estado || '').toLowerCase() === 'activo')
    .filter(c => { const v = venceNorm(c.vence); return !v || v >= hoy; })
    .map(c => ({
      cupon_id: c.cupon_id,
      monto_cents: Number(c.monto_cents) || 0,
      monto_mxn: (Number(c.monto_cents) || 0) / 100,
      campana: c.campana || '',
      condicion: c.condicion || '',
      vence: venceNorm(c.vence),
      nombre: c.nombre || ''    // para la sesión de invitado (saludo del portal)
    }))
    .filter(c => c.monto_cents > 0)
    .sort((a, b) => b.monto_cents - a.monto_cents);
}

function rewardsCandadoCanje(ledger, earnedOrders) {
  const excl = ledger && ledger.exclusion;
  if (excl && excl.nivel === 'bloqueado') {
    return { status: 403, error: 'esta cuenta no participa en el programa por el momento; si crees que es un error, escribenos por WhatsApp' };
  }
  const MSG_ADEUDO = 'canje suspendido: hay un saldo por aclarar en tu cuenta — resuelvelo en mostrador o por WhatsApp y tus puntos te esperan (no se pierden)';
  if (excl && excl.nivel === 'adeudo') return { status: 409, error: MSG_ADEUDO };
  const hace30d = Date.now() - 30 * 86400000;
  const vencida = (earnedOrders || []).some(o =>
    o.status === 'stopped' &&
    (o.payment_status === 'payment_due' || o.payment_status === 'partially_paid') &&
    o.stops_at && new Date(o.stops_at).getTime() < hace30d);
  if (vencida) return { status: 409, error: MSG_ADEUDO };
  return null;
}

// Todo texto que acaba en una celda del Sheet: recortado y neutralizado contra
// inyección de fórmulas (un nombre de cliente que empiece con = + - @ lo
// interpretaría Sheets como fórmula). Auditoría 25-jul-2026.
function rewardsCellSafe(v) {
  if (typeof v !== 'string') return v;
  const s = v.slice(0, 300);
  return /^[=+\-@\t\r]/.test(s) ? "'" + s : s;
}

// Escribe una fila al Ledger (Apps Script doPost). true/false.
async function rewardsLedgerWrite(row) {
  if (!REWARDS_SHEETS_URL) return false;
  const safe = {};
  Object.keys(row || {}).forEach(k => { safe[k] = rewardsCellSafe(row[k]); });
  // La clave del Ledger viaja en TODA escritura (canje, scan, aplicar, notif,
  // atribucion, cupones): con ella el candado global keyOk_ del Apps Script
  // rechaza cualquier POST anónimo. Cierra el hallazgo ALTA de la auditoría
  // 19-ago-2026 (el doPost aceptaba escrituras sin clave). No sobrescribe una
  // k ya puesta por el llamador.
  if (safe.k === undefined && process.env.REWARDS_HITOS_KEY) safe.k = process.env.REWARDS_HITOS_KEY;
  try {
    const r = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(safe),
      redirect: 'follow'
    });
    if (!r.ok) { console.error('rewards ledger write failed: ' + r.status); return false; }
    const j = await r.json().catch(() => null);
    return !j || j.ok !== false;
  } catch (e) {
    console.error('rewards ledger write error: ' + e.message);
    return false;
  }
}

// Como rewardsLedgerWrite pero DEVUELVE el JSON parseado del Ledger (no un
// booleano) — para operaciones que necesitan leer la respuesta, p.ej. el claim
// atómico de un cupón (updated:true/false decide si se puede tocar Booqable).
async function rewardsLedgerCall(row) {
  if (!REWARDS_SHEETS_URL) return null;
  const safe = {};
  Object.keys(row || {}).forEach(k => { safe[k] = rewardsCellSafe(row[k]); });
  if (safe.k === undefined && process.env.REWARDS_HITOS_KEY) safe.k = process.env.REWARDS_HITOS_KEY;
  try {
    const r = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(safe), redirect: 'follow'
    });
    if (!r.ok) { console.error('rewards ledger call failed: ' + r.status); return null; }
    return await r.json().catch(() => null);
  } catch (e) {
    console.error('rewards ledger call error: ' + e.message);
    return null;
  }
}

function rewardsCleanName(name) {
  return (name || '').split('/')[0].trim();
}

// Arma la respuesta completa de miembro (usada por /member y /scan).
async function rewardsBuildMember(customer) {
  const a = customer.attributes || {};
  const earned = await rewardsComputeEarned(customer.id);
  // El teléfono de la ficha viaja al Ledger: si a este cliente se le emitió un
  // cupón por celular ANTES de tener cuenta (lead recuperado), lo ve igual.
  const pTel = (a.properties || {});
  const telFicha = rewardsPhone10(pTel.phone || pTel.phone_2 || a.phone);
  const ledger = await rewardsLedgerSummary(customer.id, a.email, telFicha);
  const redeemed = ledger ? ledger.redeemed_points : 0;
  // Atribuciones: en renta audiovisual quien ELIGE el proveedor (DP/freelance) no
  // siempre es quien PAGA (la productora). Si pidió que los puntos fueran suyos,
  // se los sumamos a él y se los restamos al titular de la orden. Una orden solo
  // puede estar atribuida a una persona (el Ledger lo garantiza).
  const ganadas = (ledger && ledger.atribuciones_ganadas) || [];
  const cedidas = (ledger && ledger.atribuciones_cedidas) || [];
  const puntosGanados = ganadas.reduce((s, x) => s + (Number(x.puntos) || 0), 0);
  const puntosCedidos = cedidas.reduce((s, x) => s + (Number(x.puntos) || 0), 0);
  const earnedTotal = Math.max(0, earned.points_earned + puntosGanados - puntosCedidos);
  const available = Math.max(0, earnedTotal - redeemed);
  const tier = rewardsTierFor(earned.revenue_12m_cents, earned.rentas_12m);
  // Ticket promedio para el crédito calibrado: revenue SIN exclusiones ELSEPC
  // (el mismo que ya calcula rewardsComputeEarned) / # de órdenes contables.
  const countableOrders = earned.orders.length;
  const avgTicketCents = countableOrders ? Math.round(earned.revenue_cents / countableOrders) : 0;
  return {
    member: {
      customer_id: customer.id,
      name: rewardsCleanName(a.name),
      full_name: a.name || '',
      email: a.email || '',
      member_id: 'FLM-' + String(a.number || '0').padStart(5, '0'),
      qr_code: rewardsQrCode(customer.id),
      // exclusión vigente (tab Exclusiones del Ledger) — el portal la ignora;
      // el mostrador la puede mostrar para que el staff sepa el motivo
      exclusion: (ledger && ledger.exclusion) || null,
      // cupones promocionales VIGENTES (crédito en pesos, con su propio
      // vencimiento y condición); el portal los pinta junto a los puntos.
      cupones: rewardsCuponesVigentes(ledger && ledger.cupones),
      // vigencia §6: limite = fecha en que caduca el saldo si no vuelve a rentar
      vigencia: earned.vigencia || null,
      member_since: a.created_at,
      last_order_at: a.last_order_at || a.latest_order_at || null,
      order_count: a.order_count || 0,
      avg_order_cents: a.average_order_value_in_cents || 0,
      avg_ticket_cents: avgTicketCents
    },
    points: {
      earned: earnedTotal,                    // ya incluye atribuciones
      earned_propias: earned.points_earned,   // solo sus órdenes en Booqable
      ganados_por_atribucion: puntosGanados,  // rentas que pidió y pagó otro
      cedidos_por_atribucion: puntosCedidos,  // rentas suyas que pidió otro
      redeemed: redeemed,
      available: available,
      revenue_cents: earned.revenue_cents,
      revenue_12m_cents: earned.revenue_12m_cents,
      elsepc_excluded_cents: earned.elsepc_excluded_cents
    },
    atribuciones: { ganadas: ganadas, cedidas: cedidas },
    tier: {
      name: tier.name,
      discount: tier.discount,
      mult: tier.mult,
      base_12m_cents: earned.revenue_12m_cents,
      rentas_12m: earned.rentas_12m
    },
    orders: earned.orders,
    redemptions: ledger ? ledger.redemptions : [],
    catalog: rewardsCatalogFor(avgTicketCents),
    ledger_ok: !!ledger
  };
}

// ── POST /rewards/beacon ─────────────────────────────────────
// v8.39 (26-ago-2026, pedido de Daniel): registrar que la página del portal se
// ABRIÓ, antes del login, para medir el embudo real (aperturas vs logins) y
// distinguir "no llegan a la página" de "llegan y el login los tumba".
// Sin datos personales: solo hash(IP+día) para deduplicar 1 fila por visitante
// por día. Se guarda en el tab Visitas del Ledger con nombre "(beacon <src>)"
// — los consumidores (reporte CEO, monitor semanal) las cuentan aparte.
app.post('/rewards/beacon', (req, res) => {
  res.json({ ok: true });   // responder de inmediato; el registro es fire-and-forget
  try {
    const visOrigin = String(req.headers.origin || '');
    if (!REWARDS_SHEETS_URL || !process.env.REWARDS_HITOS_KEY || !REWARDS_ORIGINS.includes(visOrigin)) return;
    const src = (String((req.body || {}).src || 'directo').replace(/[^a-z0-9_-]/gi, '').slice(0, 20)) || 'directo';
    const hoyMty = new Date(Date.now() - 6 * 3600 * 1000).toISOString().slice(0, 10);
    const nodeCrypto = require('crypto');
    const iphash = nodeCrypto.createHash('sha1').update(rewardsClientIp(req) + '|' + hoyMty).digest('hex').slice(0, 10);
    fetch(REWARDS_SHEETS_URL, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tipo: 'visita', k: process.env.REWARDS_HITOS_KEY,
        clave: 'beacon|' + src + '|' + iphash + '|' + hoyMty,
        customer_id: '', email: '', nombre: '(beacon ' + src + ')'
      }),
      redirect: 'follow'
    }).catch(() => { /* el beacon nunca es crítico */ });
  } catch (e) { /* nunca crítico */ }
});

// ── GET /rewards/member?email=  |  ?customer_id= + token ────
// v8.28.0: dos puertas de entrada. La de siempre (correo) y la del login por
// WhatsApp, que entrega un customer_id respaldado por un token firmado. En la
// vía customer_id el token se exige SIEMPRE: el id viaja dentro del QR del
// miembro, así que sin firma sería una llave pública a los datos de cualquiera.
app.get('/rewards/member', async (req, res) => {
  const email = String(req.query.email || '').trim().toLowerCase();
  const porId = String(req.query.customer_id || '').trim();
  // v8.36: vista de INVITADO — un lead con cupón por teléfono y sin cuenta.
  // Exige sesión OTP válida (el token trae el teléfono verificado); devuelve
  // SOLO sus cupones, nada de Booqable.
  if (String(req.query.guest || '') === '1') {
    const sesInv = rewardsSesionDe(req);
    if (!sesInv || !sesInv.phone) return res.status(401).json({ ok: false, error: 'sesion invalida o vencida, vuelve a entrar' });
    const cuponesInv = await rewardsCuponesPorTelefono(String(sesInv.phone));
    return res.json({
      ok: true, guest: true,
      member: {
        name: (cuponesInv[0] && cuponesInv[0].nombre) || 'Cliente Filmorent',
        cupones: cuponesInv
      }
    });
  }
  if (!email && !porId) return res.status(400).json({ ok: false, error: 'falta email o customer_id' });
  if (!porId && email.indexOf('@') === -1) {
    return res.status(400).json({ ok: false, error: 'email invalido' });
  }
  if (porId) {
    const ses = rewardsSesionDe(req);
    if (!ses || ses.ids.indexOf(porId) === -1) {
      return res.status(401).json({ ok: false, error: 'sesion invalida o vencida, vuelve a entrar' });
    }
  }
  try {
    const customer = porId ? await rewardsCustomerById(porId) : await rewardsFindCustomer(email);
    if (!customer) return res.status(404).json({ ok: false, error: porId ? 'no existe esa cuenta' : 'no existe cuenta con ese email' });
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customer.id)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }
    const out = await rewardsBuildMember(customer);
    console.log('[rewards] member ' + (email || 'id:' + porId) + ' -> ' + out.points.available + ' pts disponibles (' +
      out.points.earned + ' ganados, ' + out.points.redeemed + ' canjeados, ledger=' + out.ledger_ok + ')');
    // Contador de visitas al portal (pedido de Daniel 6-ago-2026): cuenta SOLO si
    // la consulta viene del portal (header Origin de nuestros dominios) — los
    // curls de monitoreo y el mostrador no ensucian el dato. Una por cliente por
    // día (dedupe en el Ledger) y fire-and-forget: jamás frena la respuesta.
    const visOrigin = String(req.headers.origin || '');
    if (REWARDS_SHEETS_URL && process.env.REWARDS_HITOS_KEY && REWARDS_ORIGINS.includes(visOrigin)) {
      const hoyMty = new Date(Date.now() - 6 * 3600 * 1000).toISOString().slice(0, 10);
      fetch(REWARDS_SHEETS_URL, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tipo: 'visita', k: process.env.REWARDS_HITOS_KEY,
          clave: customer.id + '|' + hoyMty,
          customer_id: customer.id, email: email || String((customer.attributes || {}).email || ''),
          nombre: rewardsCellSafe(rewardsCleanName((customer.attributes || {}).name))
        }),
        redirect: 'follow'
      }).catch(() => { /* la visita nunca es crítica */ });
    }
    return res.json(Object.assign({ ok: true }, out));
  } catch (e) {
    console.error('[rewards] member error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error consultando Booqable, intenta de nuevo' });
  }
});

// ============================================================
// v8.28.0 LOGIN DEL CLIENTE POR WHATSAPP (OTP) — 7-ago-2026
// Por qué: el portal identifica al miembro por el correo de su ficha de
// Booqable, y 405 de los 1,987 clientes con órdenes (20.8%) están registrados
// con un correo corporativo que su empresa puede apagar. Caso que lo destapó:
// Christian Manaure (Oro, 2,771 pts) no podía entrar porque Multimedios ya dio
// de baja su correo. El teléfono sobrevive al cambio de trabajo y la cobertura
// es la misma: 97.8% de los clientes con órdenes tiene teléfono vs 98.0% con
// correo (medido sobre los 3,869 clientes de Booqable, 7-ago-2026).
//
// El login por correo SE CONSERVA — esto es una segunda puerta, no un
// reemplazo: 29 clientes activos tienen correo sin teléfono y 25 al revés.
//
//   POST /rewards/otp/solicitar {phone}        -> manda el código por WhatsApp
//   POST /rewards/otp/verificar {phone, code}  -> {token, cuentas:[...]}
//   GET  /rewards/member?customer_id=&token    -> igual que por correo
//
// El código va en la plantilla `rewards_codigo_acceso` (categoría
// Authentication, es), aprobada por Meta el 7-ago-2026. Fuera de la ventana de
// 24 h WhatsApp SOLO entrega plantillas aprobadas, por eso no se manda texto.
// El payload de la plantilla es el que da Respond.io en "Copy API Payload".
// ============================================================

const REWARDS_OTP_SECRET = process.env.REWARDS_OTP_SECRET || '';
const REWARDS_OTP_TEMPLATE = process.env.REWARDS_OTP_TEMPLATE || 'rewards_codigo_acceso';
const REWARDS_OTP_CHANNEL = parseInt(process.env.REWARDS_OTP_CHANNEL || '469627', 10);
const REWARDS_OTP_TTL_MS = 10 * 60 * 1000;   // igual que la expiración de la plantilla
const REWARDS_OTP_MAX_INTENTOS = 5;          // por código
const REWARDS_OTP_MAX_ENVIOS = 3;            // por teléfono por hora (cada envío cuesta)
const REWARDS_SESION_DIAS = 30;              // el portal guarda el token en el teléfono
// phone10 -> {code, exp, intentos, envios:[timestamps]}. En memoria a propósito:
// si Render reinicia, el código en vuelo se pierde y el cliente pide otro. Un
// OTP que sobrevive al reinicio del server es un OTP que vive de más.
const rewardsOtps = new Map();

// Últimos 10 dígitos: así se guarda el teléfono en Booqable con y sin lada.
function rewardsPhone10(raw) {
  const d = String(raw || '').replace(/\D/g, '');
  return d.length >= 10 ? d.slice(-10) : '';
}

function rewardsCustomerById(id) {
  return booqableGet('/customers/' + encodeURIComponent(id))
    .then(d => (d && d.data) || null)
    .catch(e => { if (e.status === 404) return null; throw e; });
}

// Clientes cuyo teléfono termina en estos 10 dígitos. filter[q] busca en varios
// campos a la vez, así que el match se CONFIRMA contra properties.phone/phone_2
// (mismo candado que usa el copiloto de órdenes). Puede devolver más de uno:
// 23 teléfonos están compartidos por 2+ clientes (46 clientes, 2.3%), casi
// siempre fichas duplicadas de la misma persona. No se adivina: se pregunta.
async function rewardsCustomersByPhone(phone10) {
  if (!phone10) return [];
  // ~862 de 3,720 fichas guardan el teléfono FORMATEADO ('81 1570 9932') y
  // filter[q] con los 10 dígitos pegados NO las encuentra (medido en vivo,
  // review 10-ago-2026). La variante 'XXXX XXXX' (grupos 2º y 3º) encontró
  // 8/8 fichas formateadas reales. Se intentan variantes en orden; el candado
  // sigue siendo el post-filtro por dígitos EXACTOS — jamás se adivina.
  const variantes = [phone10, phone10.slice(2, 6) + ' ' + phone10.slice(6)];
  for (const v of variantes) {
    const d = await booqableGet('/customers?filter[q]=' + encodeURIComponent(v) + '&page[size]=25');
    const buenos = (d.data || []).filter(c => {
      const a = c.attributes || {};
      if (a.archived) return false;
      if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(c.id)) return false;
      const p = a.properties || {};
      return [p.phone, p.phone_2].some(t => {
        const dd = String(t || '').replace(/\D/g, '');
        return dd && dd.slice(-10) === phone10;
      });
    });
    if (buenos.length) return buenos;
  }
  return [];
}

// Token de sesión propio (HMAC), no un JWT de librería: no se agregan
// dependencias al package.json de producción. Trae los customer_id que ese
// teléfono puede abrir — el server nunca confía en el id que manda el portal.
function rewardsSesionFirmar(phone10, ids) {
  const nodeCrypto = require('crypto');
  const payload = Buffer.from(JSON.stringify({
    p: phone10, ids: ids, exp: Date.now() + REWARDS_SESION_DIAS * 86400000
  })).toString('base64url');
  const firma = nodeCrypto.createHmac('sha256', REWARDS_OTP_SECRET).update(payload).digest('base64url');
  return payload + '.' + firma;
}

function rewardsSesionAbrir(token) {
  if (!REWARDS_OTP_SECRET) return null;
  const partes = String(token || '').split('.');
  if (partes.length !== 2) return null;
  const nodeCrypto = require('crypto');
  const esperada = nodeCrypto.createHmac('sha256', REWARDS_OTP_SECRET).update(partes[0]).digest('base64url');
  const a = Buffer.from(partes[1]); const b = Buffer.from(esperada);
  if (a.length !== b.length || !nodeCrypto.timingSafeEqual(a, b)) return null;
  let datos = null;
  try { datos = JSON.parse(Buffer.from(partes[0], 'base64url').toString('utf8')); } catch (e) { return null; }
  if (!datos || !Array.isArray(datos.ids) || !datos.exp || Date.now() > datos.exp) return null;
  return { phone: datos.p, ids: datos.ids };
}

// El token viaja en el header Authorization (fuera de la URL, igual que el del
// staff); se acepta ?token= como respaldo para el <img> del QR y pruebas.
function rewardsSesionDe(req) {
  const h = String(req.headers.authorization || '');
  const tok = h.indexOf('Bearer ') === 0 ? h.slice(7).trim() : String(req.query.token || '').trim();
  return tok ? rewardsSesionAbrir(tok) : null;
}

async function respondPost(ruta, body) {
  const r = await fetch('https://api.respond.io/v2' + ruta, {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  const txt = await r.text().catch(() => '');
  let j = null; try { j = JSON.parse(txt); } catch (e) { /* respuesta no-JSON */ }
  return { ok: r.ok, status: r.status, body: j, raw: txt.slice(0, 300) };
}

async function respondGet(ruta) {
  const r = await fetch('https://api.respond.io/v2' + ruta, {
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY }
  });
  const txt = await r.text().catch(() => '');
  let j = null; try { j = JSON.parse(txt); } catch (e) { /* respuesta no-JSON */ }
  return { ok: r.ok, status: r.status, body: j, raw: txt.slice(0, 300) };
}

// Tope GLOBAL de envíos por hora (review 10-ago-2026): el rate por IP se evade
// rotando x-forwarded-for (el portal pega directo al origin), y los límites
// por-clave solo frenan por-víctima. Sin un tope total, un atacante puede
// agotar la cuota diaria de MailApp del Ledger (tumba TODOS los correos de
// Rewards) o enumerar clientes. Estos topes son el freno cross-cuenta; a
// escala piloto ningún tráfico legítimo se les acerca. Chequeo y cobro
// SEPARADOS (2ª verificación): un envío que falla downstream no debe comerse
// la cuota de los legítimos. Riesgo residual aceptado y documentado: un
// atacante persistente puede quemar la cubeta compartida (503 temporal para
// todos) — preferible a que queme la cuota de MailApp, y reversible subiendo
// el tope por env sin deploy de código.
const rewardsGlobalEnvios = new Map(); // tipo -> [timestamps]
function rewardsGlobalLleno(tipo, maxPorHora) {
  const ahora = Date.now();
  const lista = (rewardsGlobalEnvios.get(tipo) || []).filter(t => ahora - t < 3600000);
  rewardsGlobalEnvios.set(tipo, lista);
  return lista.length >= maxPorHora;
}
function rewardsGlobalCobrar(tipo) {
  const lista = rewardsGlobalEnvios.get(tipo) || [];
  lista.push(Date.now());
  rewardsGlobalEnvios.set(tipo, lista);
}

// El botón de la plantilla Authentication se manda como type 'url' con el código
// de parámetro — mandarlo como 'otp' produce el Meta #131008 ("Button at index 0
// of type Url requires a parameter"): ese fue el bug del 10-ago-2026 con el que
// NADIE podía entrar por WhatsApp. Y como Respond.io acepta el mensaje y el
// rechazo de Meta llega DESPUÉS, tras enviar se consulta el estado real unos
// segundos antes de decirle al cliente "enviado".
async function rewardsEnviarCodigo(idents, code) {
  const msg = {
    channelId: REWARDS_OTP_CHANNEL,
    message: {
      type: 'whatsapp_template',
      template: {
        name: REWARDS_OTP_TEMPLATE,
        languageCode: 'es',
        components: [
          // El "text" es lo que se ve en el Inbox de Respond.io; el que entrega
          // WhatsApp es el de la plantilla aprobada, no éste.
          { type: 'body', text: code + ' es tu código de acceso a Filmorent Rewards.', parameters: [{ type: 'text', text: code }] },
          { type: 'buttons', buttons: [{ type: 'url', parameters: [{ type: 'text', text: code }] }] }
        ]
      }
    }
  };
  let ultimo = 'sin intentos';
  for (const ident of idents) {
    const ruta = '/contact/' + encodeURIComponent(ident) + '/message';
    let r = await respondPost(ruta, msg);
    if (!r.ok && r.status === 404) {
      // Nunca nos ha escrito: se da de alta el contacto y se reintenta una vez.
      await respondPost('/contact/create_or_update/' + encodeURIComponent(ident), { phone: ident.slice(6) });
      r = await respondPost(ruta, msg);
    }
    if (!r.ok) {
      ultimo = r.status + ' ' + ((r.body && (r.body.message || r.body.error)) || r.raw);
      continue;
    }
    // Nota interna para el equipo (pedido de Daniel 10-ago-2026): las plantillas
    // por API se ven como "unsupported" en el Inbox y nadie sabe qué se mandó.
    // Fire-and-forget: el aviso jamás frena el login. El código NO va en la nota.
    const avisoInterno = () => {
      respondPost('/contact/' + encodeURIComponent(ident) + '/comment', {
        text: '🤖 Rewards: se le envió por API su código de acceso al portal (plantilla rewards_codigo_acceso, login por WhatsApp). En el Inbox se ve como "unsupported", pero al cliente le llega normal.'
      }).catch(() => { /* nunca es crítico */ });
    };
    const messageId = r.body && r.body.messageId;
    if (!messageId) { avisoInterno(); return { ok: true, identificador: ident, verificado: false }; }
    let fallo = null;
    for (let i = 0; i < 4; i++) {
      await new Promise(z => setTimeout(z, 1300));
      // GET de mensaje individual: mismo endpoint que usa el MCP de Respond.io
      // (verificado en vivo 10-ago-2026, regresó {status:[...]} en la raíz);
      // se acepta también la forma {data:{...}} por si la API la envuelve, y
      // si no viene status se loguea — nunca fallar en silencio.
      const q = await respondGet('/contact/' + encodeURIComponent(ident) + '/message/' + messageId);
      const cuerpoMsg = (q.body && (q.body.data || q.body)) || {};
      const lista = Array.isArray(cuerpoMsg.status) ? cuerpoMsg.status : [];
      if (!lista.length && i === 3) {
        console.error('[rewards] verificacion de entrega sin status (' + q.status + '): ' + String(q.raw).slice(0, 120));
      }
      if (lista.some(s => s.value === 'failed')) {
        fallo = (lista.filter(s => s.value === 'failed')[0] || {}).message || 'failed';
        break;
      }
      if (lista.some(s => s.value === 'sent' || s.value === 'delivered' || s.value === 'read')) {
        avisoInterno();
        return { ok: true, identificador: ident, verificado: true };
      }
    }
    if (fallo) { ultimo = 'WhatsApp: ' + fallo; continue; }
    // Sigue "pending" tras ~5s: beneficio de la duda (el rechazo de Meta llega
    // en <1s; un pending largo suele ser WhatsApp lento, no un error).
    avisoInterno();
    return { ok: true, identificador: ident, verificado: false };
  }
  return { ok: false, error: ultimo };
}

// A qué identificador de WhatsApp mandar el código (review 10-ago-2026):
// - UN solo número completo con lada y NO es de México → SOLO ese (jamás caer
//   a un +52 "inventado": ese WhatsApp sería de un desconocido en México).
// - Cero o varios números completos distintos con el mismo last-10 → solo los
//   formatos de México (+52 y el viejo +521), que apuntan a quien tecleó.
function rewardsIdentsPara(clientes, phone10) {
  const completos = [];
  for (const c of clientes) {
    const props = (c.attributes || {}).properties || {};
    for (const t of [props.phone, props.phone_2]) {
      const dd = String(t || '').replace(/\D/g, '');
      if (dd.length > 10 && dd.slice(-10) === phone10 && completos.indexOf(dd) === -1) completos.push(dd);
    }
  }
  // Solo se confía en el número de la ficha cuando es INEQUÍVOCO (2ª
  // verificación del review): UNA sola ficha matcheada, UN solo número
  // completo, lada real extranjera (ni '52' ni prefijos nacionales viejos
  // '01'/'045', que empiezan con 0). Con 2+ fichas el código abriría una
  // sesión multi-cuenta enviada al teléfono de OTRO — jamás adivinar.
  if (completos.length === 1 && clientes.length === 1 &&
      completos[0].slice(0, 2) !== '52' && completos[0].charAt(0) !== '0') {
    return { idents: ['phone:+' + completos[0]], extranjero: true };
  }
  return { idents: ['phone:+52' + phone10, 'phone:+521' + phone10], extranjero: false };
}

// ── POST /rewards/otp/solicitar  {phone} ────────────────────
// Cupones VIGENTES amarrados a un teléfono (v8.36) — para que un lead que aún
// no es cliente pueda entrar al portal y ver su crédito promocional. Devuelve
// [] si no hay o si el Ledger falla (el login de cuentas no depende de esto).
async function rewardsCuponesPorTelefono(phone10) {
  if (!REWARDS_SHEETS_URL || !phone10) return [];
  try {
    // consulta sin customer_id → lleva la clave (Ledger v12 la exige para
    // identidades adivinables como el celular)
    const r = await fetch(REWARDS_SHEETS_URL + '?action=member&telefono=' + encodeURIComponent(phone10) +
      (process.env.REWARDS_HITOS_KEY ? '&k=' + encodeURIComponent(process.env.REWARDS_HITOS_KEY) : ''), { redirect: 'follow' });
    if (!r.ok) return [];
    const j = await r.json().catch(() => null);
    if (!j || j.ok === false) return [];
    return rewardsCuponesVigentes(j.cupones);
  } catch (e) {
    console.error('rewards cupones por telefono error: ' + e.message);
    return [];
  }
}

app.post('/rewards/otp/solicitar', async (req, res) => {
  if (!REWARDS_OTP_SECRET || !RESPONDIO_API_KEY) {
    return res.status(503).json({ ok: false, error: 'login por WhatsApp deshabilitado (falta REWARDS_OTP_SECRET o RESPONDIO_API_KEY)' });
  }
  const phone10 = rewardsPhone10((req.body || {}).phone);
  if (!phone10) return res.status(400).json({ ok: false, error: 'escribe tu celular a 10 dígitos' });
  const ahora = Date.now();
  const previo = rewardsOtps.get(phone10);
  const envios = ((previo && previo.envios) || []).filter(t => ahora - t < 3600000);
  if (envios.length >= REWARDS_OTP_MAX_ENVIOS) {
    return res.status(429).json({ ok: false, error: 'ya te mandamos varios códigos, espera una hora o entra con tu correo' });
  }
  try {
    const clientes = await rewardsCustomersByPhone(phone10);
    // Mismo criterio que el login por correo: se dice que no hay cuenta para que
    // el portal pueda ofrecer la otra puerta en vez de dejar al cliente colgado.
    // EXCEPCIÓN v8.36: si ese celular tiene un CUPÓN vigente (lead de promo que
    // aún no renta), sí se le manda código — entra como invitado a ver su
    // crédito. El código llega a SU teléfono, así que no hay fuga de datos.
    let esInvitado = false;
    if (!clientes.length) {
      const cuponesTel = await rewardsCuponesPorTelefono(phone10);
      if (!cuponesTel.length) {
        return res.status(404).json({ ok: false, error: 'no encontramos una cuenta con ese teléfono' });
      }
      esInvitado = true;
    }
    const nodeCrypto = require('crypto');
    const code = String(nodeCrypto.randomInt(0, 1000000)).padStart(6, '0');
    envios.push(ahora);
    rewardsOtps.set(phone10, { code: code, exp: ahora + REWARDS_OTP_TTL_MS, intentos: 0, envios: envios });
    if (rewardsOtps.size > 5000) { // tope de memoria, igual que el rate limit
      for (const [k, v] of rewardsOtps) { if (v.exp < ahora) rewardsOtps.delete(k); }
    }
    if (rewardsGlobalLleno('otp', 60)) {
      rewardsOtps.delete(phone10);
      return res.status(503).json({ ok: false, error: 'estamos mandando muchos códigos en este momento; intenta en unos minutos o entra con tu correo' });
    }
    const eleccion = rewardsIdentsPara(clientes, phone10);
    const envio = await rewardsEnviarCodigo(eleccion.idents, code);
    if (envio.ok) rewardsGlobalCobrar('otp');
    if (!envio.ok) {
      // Nunca fallar en silencio: si WhatsApp no lo entregó, el cliente tiene que
      // enterarse en la misma pantalla, no quedarse esperando un código fantasma.
      console.error('[rewards] otp no enviado a ...' + phone10.slice(-4) + ': ' + envio.error);
      rewardsOtps.delete(phone10);
      return res.status(502).json({ ok: false, error: 'no pudimos mandarte el WhatsApp, intenta de nuevo o entra con tu correo' });
    }
    console.log('[rewards] otp enviado a ...' + phone10.slice(-4) + ' (' + clientes.length + ' cuenta(s) con ese tel)');
    return res.json({
      ok: true,
      telefono: '••• ••• ' + phone10.slice(-4),
      expira_min: Math.round(REWARDS_OTP_TTL_MS / 60000)
    });
  } catch (e) {
    console.error('[rewards] otp solicitar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error consultando Booqable, intenta de nuevo' });
  }
});

// ── POST /rewards/otp/verificar  {phone, code} ──────────────
app.post('/rewards/otp/verificar', async (req, res) => {
  if (!REWARDS_OTP_SECRET) return res.status(503).json({ ok: false, error: 'login por WhatsApp deshabilitado' });
  const phone10 = rewardsPhone10((req.body || {}).phone);
  const code = String((req.body || {}).code || '').replace(/\D/g, '');
  if (!phone10 || code.length !== 6) return res.status(400).json({ ok: false, error: 'código de 6 dígitos' });
  const reg = rewardsOtps.get(phone10);
  if (!reg || Date.now() > reg.exp) {
    rewardsOtps.delete(phone10);
    return res.status(400).json({ ok: false, error: 'el código venció, pide uno nuevo' });
  }
  reg.intentos++;
  if (reg.intentos > REWARDS_OTP_MAX_INTENTOS) {
    rewardsOtps.delete(phone10);
    return res.status(429).json({ ok: false, error: 'demasiados intentos, pide un código nuevo' });
  }
  const nodeCrypto = require('crypto');
  const a = Buffer.from(code); const b = Buffer.from(reg.code);
  if (a.length !== b.length || !nodeCrypto.timingSafeEqual(a, b)) {
    return res.status(401).json({ ok: false, error: 'código incorrecto', intentos_restantes: REWARDS_OTP_MAX_INTENTOS - reg.intentos });
  }
  rewardsOtps.delete(phone10); // un código, un uso
  try {
    const clientes = await rewardsCustomersByPhone(phone10);
    if (!clientes.length) {
      // v8.36: lead con cupón y sin cuenta → sesión de INVITADO (ids vacíos).
      // Solo puede ver sus cupones; ninguna cuenta de Booqable se le abre.
      const cuponesTel = await rewardsCuponesPorTelefono(phone10);
      if (!cuponesTel.length) return res.status(404).json({ ok: false, error: 'no encontramos una cuenta con ese teléfono' });
      const tokenInv = rewardsSesionFirmar(phone10, []);
      console.log('[rewards] otp ok ...' + phone10.slice(-4) + ' -> INVITADO con ' + cuponesTel.length + ' cupon(es)');
      return res.json({
        ok: true, token: tokenInv, dias: REWARDS_SESION_DIAS, cuentas: [],
        guest: true, nombre: (cuponesTel[0] && cuponesTel[0].nombre) || ''
      });
    }
    // Más rentas primero: cuando hay ficha duplicada, la buena suele ser esa.
    clientes.sort((x, y) => ((y.attributes || {}).order_count || 0) - ((x.attributes || {}).order_count || 0));
    const cuentas = clientes.map(c => {
      const a2 = c.attributes || {};
      return {
        customer_id: c.id,
        nombre: rewardsCleanName(a2.name),
        email: a2.email || '',              // ya se autenticó: ver su propio correo es el punto
        order_count: a2.order_count || 0,
        last_order_at: a2.latest_order_at || null
      };
    });
    const token = rewardsSesionFirmar(phone10, cuentas.map(c => c.customer_id));
    console.log('[rewards] otp ok ...' + phone10.slice(-4) + ' -> ' + cuentas.length + ' cuenta(s)');
    return res.json({ ok: true, token: token, dias: REWARDS_SESION_DIAS, cuentas: cuentas });
  } catch (e) {
    console.error('[rewards] otp verificar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error consultando Booqable, intenta de nuevo' });
  }
});

// ============================================================
// VINCULAR CELULAR (10-ago-2026, pedido de Daniel tras el caso
// Christian/Sultanes y el suyo propio: el celular del cliente no
// está en su ficha de Booqable y el login por WhatsApp no lo
// encuentra). DOBLE CANAL, decidido en la revisión adversarial:
//   1. POST /rewards/vincular/solicitar {email, phone} → valida
//      que el correo sea EXACTAMENTE el de la ficha y manda un
//      enlace firmado a ESE buzón (el buzón autoriza).
//   2. GET /rewards/vincular/confirmar?t=... → SOLO muestra la
//      página (un GET jamás escribe: los escáners de correo tipo
//      Safe Links abren los enlaces solos).
//   3. POST /rewards/vincular/otp {t} → manda un código de
//      WhatsApp AL CELULAR NUEVO (el celular demuestra posesión).
//   4. POST /rewards/vincular/completar {t, code} → con ambas
//      pruebas (buzón + celular) escribe la property en Booqable.
//   Y POST /rewards/vincular/staff {nombre, phone, email} → deja
//      la solicitud en el Ledger y avisa al equipo por correo.
// Sin las DOS pruebas no se liga nada: los puntos son dinero, y
// un enlace que escribiera solo (o un teléfono sin verificar)
// era un account-takeover de un clic (hallazgo del review).
// ============================================================

const REWARDS_PUBLIC_URL = process.env.REWARDS_PUBLIC_URL || 'https://filmorent-tag-analyzer.onrender.com';
const REWARDS_VINCULO_TTL_MS = 45 * 60 * 1000;  // vida del enlace del correo
const rewardsVinculosUsados = new Map();        // firma -> exp. En memoria a propósito:
// tras un reinicio el enlace "revive", pero ya no puede escribir nada sin un
// código fresco al celular — el candado real es el doble canal, no este Map.
const rewardsVinculoEnvios = new Map();         // clave -> [timestamps ultima hora]
const rewardsVinculoCodigos = new Map();        // firma -> {code, exp, intentos}

// Chequeo y cobro SEPARADOS (review): cobrar en el chequeo hacía que 3 typos
// de correo bloquearan el teléfono 1 hora con un mensaje falso.
function rewardsVinculoRateLleno(clave) {
  const ahora = Date.now();
  return ((rewardsVinculoEnvios.get(clave) || []).filter(t => ahora - t < 3600000)).length >= 3;
}
function rewardsVinculoRateCobrar(clave) {
  const ahora = Date.now();
  const lista = (rewardsVinculoEnvios.get(clave) || []).filter(t => ahora - t < 3600000);
  lista.push(ahora);
  rewardsVinculoEnvios.set(clave, lista);
  if (rewardsVinculoEnvios.size > 5000) {
    for (const [k, v] of rewardsVinculoEnvios) {
      if (!v.some(t => ahora - t < 3600000)) rewardsVinculoEnvios.delete(k);
    }
  }
}

// Mismo esquema HMAC que la sesión del OTP; namespace propio para que un token
// de vínculo jamás sirva como sesión ni al revés.
function rewardsVinculoFirmar(customerId, phoneGuardar) {
  const nodeCrypto = require('crypto');
  const payload = Buffer.from(JSON.stringify({
    c: customerId, t: phoneGuardar, a: 'vinculo', exp: Date.now() + REWARDS_VINCULO_TTL_MS
  })).toString('base64url');
  const firma = nodeCrypto.createHmac('sha256', REWARDS_OTP_SECRET).update('vinculo.' + payload).digest('base64url');
  return payload + '.' + firma;
}

function rewardsVinculoAbrir(token) {
  const nodeCrypto = require('crypto');
  const partes = String(token || '').split('.');
  if (partes.length !== 2 || !partes[0] || !partes[1]) return null;
  const esperada = nodeCrypto.createHmac('sha256', REWARDS_OTP_SECRET).update('vinculo.' + partes[0]).digest('base64url');
  const a = Buffer.from(partes[1]); const b = Buffer.from(esperada);
  if (a.length !== b.length || !nodeCrypto.timingSafeEqual(a, b)) return null;
  let datos = null;
  try { datos = JSON.parse(Buffer.from(partes[0], 'base64url').toString()); } catch (e) { return null; }
  if (!datos || datos.a !== 'vinculo' || !datos.exp || Date.now() > datos.exp) return null;
  return datos;
}

// El celular como se va a GUARDAR en la ficha (review: la versión anterior le
// inventaba lada de Japón al dedazo '81123456789'). Sin '+': solo 10 dígitos
// exactos, o 12-13 que empiecen con 52 (México sin signo). Con '+': 11-15.
// Cualquier otra cosa se rechaza — nunca se le inventa el país a un número.
function rewardsVinculoPhone(raw) {
  const limpio = String(raw || '').trim();
  const digitos = limpio.replace(/\D/g, '');
  if (limpio.charAt(0) === '+') {
    return (digitos.length >= 11 && digitos.length <= 15) ? ('+' + digitos) : null;
  }
  if (digitos.length === 10) return digitos;
  if ((digitos.length === 12 && digitos.slice(0, 2) === '52') ||
      (digitos.length === 13 && digitos.slice(0, 3) === '521')) return '+' + digitos;
  return null;
}

const REWARDS_VINCULO_MSG_TEL = 'escribe tu celular a 10 dígitos, o completo con + y lada de país si no es de México';

// ── POST /rewards/vincular/solicitar  {email, phone} ────────
app.post('/rewards/vincular/solicitar', async (req, res) => {
  if (!REWARDS_OTP_SECRET) return res.status(503).json({ ok: false, error: 'vinculación deshabilitada (falta REWARDS_OTP_SECRET)' });
  if (!REWARDS_SHEETS_URL || !process.env.REWARDS_HITOS_KEY) return res.status(503).json({ ok: false, error: 'correo de vinculación deshabilitado (Ledger no configurado)' });
  const email = String((req.body || {}).email || '').trim().toLowerCase();
  const phone = rewardsVinculoPhone((req.body || {}).phone);
  if (!email || email.indexOf('@') === -1) return res.status(400).json({ ok: false, error: 'escribe tu correo registrado en Filmorent' });
  if (!phone) return res.status(400).json({ ok: false, error: REWARDS_VINCULO_MSG_TEL });
  if (rewardsVinculoRateLleno('e:' + email) || rewardsVinculoRateLleno('t:' + phone)) {
    return res.status(429).json({ ok: false, error: 'hiciste varios intentos seguidos; espera una hora, o toca "Ya no tengo acceso a ese correo" para avisarle al equipo' });
  }
  try {
    const customer = await rewardsFindCustomer(email);
    // El correo debe ser EXACTAMENTE el de la ficha (review: sin esto, un match
    // laxo de Booqable podría mandar el enlace a un buzón que no es el dueño).
    const emailFicha = String(((customer || {}).attributes || {}).email || '').trim().toLowerCase();
    if (!customer || (customer.attributes || {}).archived || emailFicha !== email) {
      return res.status(404).json({ ok: false, error: 'ese correo no está registrado tal cual en Filmorent — revisa que esté bien escrito, o toca "Ya no tengo acceso a ese correo" para avisarle al equipo' });
    }
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customer.id)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }
    const props = (customer.attributes || {}).properties || {};
    const p10 = phone.replace(/\D/g, '').slice(-10);
    const yaLigado = [props.phone, props.phone_2].some(t => String(t || '').replace(/\D/g, '').slice(-10) === p10);
    if (yaLigado) {
      return res.json({ ok: true, ya_ligado: true, mensaje: 'ese celular ya está en tu cuenta — pide tu código por WhatsApp directamente' });
    }
    if (props.phone && props.phone_2) {
      return res.status(409).json({ ok: false, error: 'tu cuenta ya tiene 2 teléfonos registrados; toca "Ya no tengo acceso a ese correo" para que el equipo la actualice' });
    }
    if (rewardsGlobalLleno('correo_vinculo', 20)) {
      return res.status(503).json({ ok: false, error: 'estamos recibiendo muchas solicitudes; intenta en un rato, o toca "Ya no tengo acceso a ese correo" para avisarle al equipo' });
    }
    const token = rewardsVinculoFirmar(customer.id, phone);
    const url = REWARDS_PUBLIC_URL + '/rewards/vincular/confirmar?t=' + encodeURIComponent(token);
    const r = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tipo: 'correo_vinculo', k: process.env.REWARDS_HITOS_KEY,
        email: email,
        nombre: rewardsCellSafe(rewardsCleanName((customer.attributes || {}).name)),
        // COMPLETO a propósito (review): si alguien pidió ligar un número que no
        // es tuyo, tienes que poder verlo y NO confirmar.
        telefono: phone,
        url: url
      }),
      redirect: 'follow'
    });
    const j = await r.json().catch(() => null);
    if (!j || !j.ok) {
      console.error('[rewards] vinculo: el Ledger no mandó el correo a ' + email + ': ' + JSON.stringify(j || {}).slice(0, 200));
      return res.status(502).json({ ok: false, error: 'no pudimos mandarte el correo; toca "Ya no tengo acceso a ese correo" y el equipo lo liga a mano' });
    }
    rewardsVinculoRateCobrar('e:' + email);
    rewardsVinculoRateCobrar('t:' + phone);
    rewardsGlobalCobrar('correo_vinculo');
    console.log('[rewards] vinculo solicitado: correo a ' + email + ' para tel ...' + p10.slice(-4));
    return res.json({ ok: true, mensaje: 'te mandamos un enlace a ' + email + ' — ábrelo para continuar (vence en 45 min; revisa spam)' });
  } catch (e) {
    console.error('[rewards] vinculo solicitar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error consultando Booqable, intenta de nuevo' });
  }
});

// Página HTML compartida de los pasos de vínculo (viene del correo, no del portal).
function rewardsVinculoPagina(titulo, cuerpo) {
  return '<!doctype html><html lang="es"><head><meta charset="utf-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1"><title>' + titulo + '</title></head>' +
    '<body style="font-family:-apple-system,system-ui,sans-serif;background:#f5f1e8;color:#2b2b2b;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0">' +
    '<div style="max-width:420px;padding:32px;text-align:center;background:#fff;border-radius:16px;margin:16px;box-shadow:0 2px 12px rgba(0,0,0,.07)">' + cuerpo + '</div></body></html>';
}
const REWARDS_VINCULO_LINK_PORTAL = '<p style="margin-top:24px"><a href="https://rewards.filmorent.com" style="background:#c8102e;color:#fff;padding:12px 22px;border-radius:10px;text-decoration:none;font-weight:600">Ir al portal</a></p>';

// ── GET /rewards/vincular/confirmar?t=... ───────────────────
// SOLO pinta la página. La escritura vive en /completar, detrás del código.
app.get('/rewards/vincular/confirmar', async (req, res) => {
  if (!REWARDS_OTP_SECRET) return res.status(503).send(rewardsVinculoPagina('No disponible', '<h2>Servicio no disponible</h2>' + REWARDS_VINCULO_LINK_PORTAL));
  const datos = rewardsVinculoAbrir(req.query.t);
  if (!datos) {
    return res.status(400).send(rewardsVinculoPagina('Enlace vencido', '<h2>Este enlace venció o no es válido</h2><p>Pide uno nuevo desde el portal (vive 45 minutos).</p>' + REWARDS_VINCULO_LINK_PORTAL));
  }
  // datos.t viene FIRMADO por nosotros (jamás del querystring suelto); aun así
  // se re-sanitiza a formato de teléfono antes de interpolarse en el HTML.
  const telLimpio = String(datos.t).replace(/[^0-9+]/g, '');
  const cuerpo =
    '<h2>Un paso más</h2>' +
    '<p>Vas a ligar el celular <b style="white-space:nowrap">' + telLimpio + '</b> a tu cuenta de Filmorent Rewards.</p>' +
    '<p style="background:#fdf0e7;border-radius:10px;padding:10px 14px;font-size:14px">⚠️ ¿Ese número <b>no</b> es tuyo? No sigas y avísanos.</p>' +
    '<p>Te mandamos un código de WhatsApp a ese número para confirmar que es tuyo:</p>' +
    '<button id="btnCodigo" style="background:#c8102e;color:#fff;padding:12px 22px;border-radius:10px;border:0;font-weight:600;font-size:16px;cursor:pointer">M&aacute;ndame el c&oacute;digo</button>' +
    '<form id="formCodigo" style="display:none;margin-top:16px" onsubmit="return completar(event)">' +
    '<input id="code" inputmode="numeric" maxlength="6" placeholder="123456" style="text-align:center;letter-spacing:6px;font-size:20px;padding:10px;border:1px solid #ddd;border-radius:10px;width:180px"><br>' +
    '<button type="submit" style="margin-top:12px;background:#c8102e;color:#fff;padding:12px 22px;border-radius:10px;border:0;font-weight:600;font-size:16px;cursor:pointer">Confirmar</button>' +
    '</form>' +
    '<p id="msg" style="min-height:20px;font-size:14px;color:#8a6d3b"></p>' +
    '<script>' +
    // El token ya pasó la firma HMAC (charset base64url), pero por defensa en
    // profundidad se escapa '<' para que jamás pueda romper el contexto
    // <script> (review: JSON.stringify no escapa '</script>').
    'var t=' + JSON.stringify(String(req.query.t)).replace(/</g, '\\u003c') + ';' +
    'var msg=document.getElementById("msg");' +
    'document.getElementById("btnCodigo").onclick=function(){var b=this;b.disabled=true;b.textContent="Enviando…";' +
    'fetch("/rewards/vincular/otp",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({t:t})})' +
    '.then(function(r){return r.json()}).then(function(j){' +
    'if(j.ok){msg.textContent="Código enviado — revisa tu WhatsApp.";document.getElementById("formCodigo").style.display="block";document.getElementById("code").focus();b.textContent="Mandar otro código";b.disabled=false;}' +
    'else{msg.textContent=j.error||"No pudimos mandar el código.";b.textContent="Reintentar";b.disabled=false;}' +
    '}).catch(function(){msg.textContent="Error de red, intenta de nuevo.";b.textContent="Reintentar";b.disabled=false;});};' +
    'function completar(ev){ev.preventDefault();var code=document.getElementById("code").value.replace(/\\D/g,"");' +
    'if(code.length!==6){msg.textContent="El código es de 6 dígitos.";return false;}' +
    'fetch("/rewards/vincular/completar",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({t:t,code:code})})' +
    '.then(function(r){return r.json()}).then(function(j){' +
    'if(j.ok){document.body.innerHTML=' + JSON.stringify(rewardsVinculoPagina('Listo', '<h2>✅ Listo</h2><p>Tu celular quedó ligado a tu cuenta de Filmorent Rewards.</p><p>Ya puedes entrar al portal pidiendo tu código por WhatsApp.</p>' + REWARDS_VINCULO_LINK_PORTAL)) + ';}' +
    'else{msg.textContent=j.error||"Código incorrecto.";}' +
    '}).catch(function(){msg.textContent="Error de red, intenta de nuevo.";});return false;}' +
    '<\/script>';
  return res.send(rewardsVinculoPagina('Liga tu celular', cuerpo));
});

// ── POST /rewards/vincular/otp  {t} ─────────────────────────
// Manda el código al CELULAR NUEVO del token. Prueba de posesión del teléfono.
app.post('/rewards/vincular/otp', async (req, res) => {
  if (!REWARDS_OTP_SECRET || !RESPONDIO_API_KEY) return res.status(503).json({ ok: false, error: 'servicio no disponible' });
  const datos = rewardsVinculoAbrir((req.body || {}).t);
  if (!datos) return res.status(400).json({ ok: false, error: 'el enlace venció; pide uno nuevo desde el portal' });
  const firma = String((req.body || {}).t).split('.')[1];
  const digitos = String(datos.t).replace(/\D/g, '');
  if (rewardsVinculosUsados.has(firma)) return res.status(400).json({ ok: false, error: 'este enlace ya se usó; entra al portal con tu celular' });
  // 'c:' frena por enlace; 'vd:' frena por TELÉFONO DESTINO sin importar
  // cuántos tokens tenga el solicitante (review: spam de plantilla a números
  // arbitrarios usando la cuenta propia).
  if (rewardsVinculoRateLleno('c:' + firma) || rewardsVinculoRateLleno('vd:' + digitos)) {
    return res.status(429).json({ ok: false, error: 'ya te mandamos varios códigos; espera una hora' });
  }
  if (rewardsGlobalLleno('otp', 60)) {
    return res.status(503).json({ ok: false, error: 'estamos mandando muchos códigos; intenta en unos minutos' });
  }
  try {
    // México SIEMPRE con las dos formas (+52 y el viejo +521), aunque el
    // cliente lo haya tecleado con lada — 2ª verificación: confiar solo en
    // digitos.length dejaba a los '+52...' sin el fallback +521 y el doble
    // canal se atoraba con 502 dependiendo del FORMATO que usara el cliente.
    const p10v = digitos.slice(-10);
    const esMx = digitos.length === 10 ||
      (digitos.length === 12 && digitos.slice(0, 2) === '52') ||
      (digitos.length === 13 && digitos.slice(0, 3) === '521');
    const idents = esMx
      ? ['phone:+52' + p10v, 'phone:+521' + p10v]
      : ['phone:+' + digitos];
    const nodeCrypto = require('crypto');
    const code = String(nodeCrypto.randomInt(0, 1000000)).padStart(6, '0');
    rewardsVinculoCodigos.set(firma, { code: code, exp: Date.now() + REWARDS_OTP_TTL_MS, intentos: 0 });
    if (rewardsVinculoCodigos.size > 2000) {
      for (const [k, v] of rewardsVinculoCodigos) { if (v.exp < Date.now()) rewardsVinculoCodigos.delete(k); }
    }
    const envio = await rewardsEnviarCodigo(idents, code);
    if (!envio.ok) {
      rewardsVinculoCodigos.delete(firma);
      console.error('[rewards] vinculo otp no enviado a ...' + digitos.slice(-4) + ': ' + envio.error);
      return res.status(502).json({ ok: false, error: 'no pudimos mandar el WhatsApp a ese número — revisa que sea correcto o avísale al equipo desde el portal' });
    }
    rewardsVinculoRateCobrar('c:' + firma);
    rewardsVinculoRateCobrar('vd:' + digitos);
    rewardsGlobalCobrar('otp');
    return res.json({ ok: true });
  } catch (e) {
    console.error('[rewards] vinculo otp error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error enviando el código, intenta de nuevo' });
  }
});

// ── POST /rewards/vincular/completar  {t, code} ─────────────
// Con las dos pruebas (buzón + celular) se escribe la property en Booqable.
app.post('/rewards/vincular/completar', async (req, res) => {
  if (!REWARDS_OTP_SECRET) return res.status(503).json({ ok: false, error: 'servicio no disponible' });
  const datos = rewardsVinculoAbrir((req.body || {}).t);
  if (!datos) return res.status(400).json({ ok: false, error: 'el enlace venció; pide uno nuevo desde el portal' });
  const firma = String((req.body || {}).t).split('.')[1];
  if (rewardsVinculosUsados.has(firma)) return res.status(400).json({ ok: false, error: 'este enlace ya se usó; entra al portal con tu celular' });
  const code = String((req.body || {}).code || '').replace(/\D/g, '');
  const reg = rewardsVinculoCodigos.get(firma);
  if (!reg || Date.now() > reg.exp) {
    rewardsVinculoCodigos.delete(firma);
    return res.status(400).json({ ok: false, error: 'el código venció, pide uno nuevo' });
  }
  reg.intentos++;
  if (reg.intentos > REWARDS_OTP_MAX_INTENTOS) {
    rewardsVinculoCodigos.delete(firma);
    return res.status(429).json({ ok: false, error: 'demasiados intentos, pide un código nuevo' });
  }
  const nodeCrypto = require('crypto');
  const a = Buffer.from(code); const b = Buffer.from(reg.code);
  if (a.length !== b.length || !nodeCrypto.timingSafeEqual(a, b)) {
    return res.status(401).json({ ok: false, error: 'código incorrecto' });
  }
  try {
    const customer = await rewardsCustomerById(datos.c);
    if (!customer || (customer.attributes || {}).archived) {
      return res.status(404).json({ ok: false, error: 'no encontramos tu cuenta; avísale al equipo desde el portal' });
    }
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customer.id)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }
    const props = (customer.attributes || {}).properties || {};
    const p10 = String(datos.t).replace(/\D/g, '').slice(-10);
    const yaLigado = [props.phone, props.phone_2].some(t => String(t || '').replace(/\D/g, '').slice(-10) === p10);
    if (!yaLigado) {
      let campo = null;
      if (!props.phone) campo = 'Phone';
      else if (!props.phone_2) campo = 'Phone 2';
      if (!campo) {
        return res.status(409).json({ ok: false, error: 'tu cuenta ya tiene 2 teléfonos; avísale al equipo desde el portal' });
      }
      await booqableWrite('POST', '/properties', {
        data: {
          type: 'properties',
          attributes: {
            name: campo, property_type: 'phone', value: datos.t,
            owner_id: customer.id, owner_type: 'customers'
          }
        }
      });
    }
    rewardsVinculoCodigos.delete(firma);
    rewardsVinculosUsados.set(firma, datos.exp);
    for (const [k, v] of rewardsVinculosUsados) { if (Date.now() > v) rewardsVinculosUsados.delete(k); }
    console.log('[rewards] vinculo COMPLETADO: tel ...' + p10.slice(-4) + ' -> ficha ' + customer.id.slice(0, 8));
    return res.json({ ok: true });
  } catch (e) {
    console.error('[rewards] vinculo completar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'algo falló de nuestro lado; intenta en un minuto' });
  }
});

// ── POST /rewards/vincular/staff  {nombre, phone, email} ────
// El cliente ya no tiene acceso al correo registrado (el caso Christian:
// se lo dieron de baja en su empresa). Queda la solicitud en el Ledger y
// el equipo verifica identidad ANTES de tocar la ficha — aquí no se
// escribe nada a Booqable.
app.post('/rewards/vincular/staff', async (req, res) => {
  if (!REWARDS_SHEETS_URL || !process.env.REWARDS_HITOS_KEY) return res.status(503).json({ ok: false, error: 'solicitudes deshabilitadas (Ledger no configurado)' });
  const nombre = String((req.body || {}).nombre || '').trim().slice(0, 120);
  const emailDicho = String((req.body || {}).email || '').trim().toLowerCase().slice(0, 120);
  const phone = rewardsVinculoPhone((req.body || {}).phone);
  if (!nombre || nombre.length < 3) return res.status(400).json({ ok: false, error: 'escribe tu nombre completo' });
  if (!phone) return res.status(400).json({ ok: false, error: REWARDS_VINCULO_MSG_TEL });
  if (rewardsVinculoRateLleno('s:' + phone)) {
    return res.status(429).json({ ok: false, error: 'ya recibimos tu solicitud; el equipo te contacta en horario hábil' });
  }
  if (rewardsGlobalLleno('solicitud_staff', 30)) {
    return res.status(503).json({ ok: false, error: 'estamos recibiendo muchas solicitudes; intenta más tarde' });
  }
  try {
    const r = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tipo: 'solicitud_vinculo', k: process.env.REWARDS_HITOS_KEY,
        nombre: rewardsCellSafe(nombre),
        email: rewardsCellSafe(emailDicho),
        telefono: rewardsCellSafe(phone),
        dispositivo: rewardsCellSafe(String(req.headers['user-agent'] || '').slice(0, 150))
      }),
      redirect: 'follow'
    });
    const j = await r.json().catch(() => null);
    if (!j || !j.ok) {
      console.error('[rewards] solicitud staff no registrada: ' + JSON.stringify(j || {}).slice(0, 200));
      return res.status(502).json({ ok: false, error: 'no pudimos registrar la solicitud, intenta de nuevo en un minuto' });
    }
    rewardsVinculoRateCobrar('s:' + phone);
    rewardsGlobalCobrar('solicitud_staff');
    console.log('[rewards] solicitud staff: ' + nombre + ' tel ...' + phone.slice(-4));
    return res.json({ ok: true, mensaje: 'listo — el equipo verifica tus datos y liga tu número en horario hábil; te avisan por WhatsApp' });
  } catch (e) {
    console.error('[rewards] solicitud staff error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'no pudimos registrar la solicitud, intenta de nuevo' });
  }
});

// ── POST /rewards/redeem  {email, reward_id} ────────────────
app.post('/rewards/redeem', async (req, res) => {
  const email = String((req.body || {}).email || '').trim().toLowerCase();
  const rewardId = parseInt((req.body || {}).reward_id, 10);
  if (!email || email.indexOf('@') === -1) return res.status(400).json({ ok: false, error: 'email invalido' });
  if (!Number.isFinite(rewardId)) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'canjes temporalmente deshabilitados (Ledger no configurado)' });

  try {
    const customer = await rewardsFindCustomer(email);
    if (!customer) return res.status(404).json({ ok: false, error: 'no existe cuenta con ese email' });
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customer.id)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }

    // Recalcular saldo Y catálogo personalizado EN VIVO (no confiar en el
    // cliente): el reward_id se valida contra el catálogo calibrado del propio
    // miembro, no contra una tabla estática.
    const earned = await rewardsComputeEarned(customer.id);
    const ledger = await rewardsLedgerSummary(customer.id, (customer.attributes || {}).email);
    if (!ledger) return res.status(503).json({ ok: false, error: 'no se pudo leer el Ledger, intenta mas tarde' });
    const candado = rewardsCandadoCanje(ledger, earned.orders);
    if (candado) return res.status(candado.status).json({ ok: false, error: candado.error });
    if (earned.vigencia && earned.vigencia.caducado) {
      return res.status(409).json({ ok: false, error: 'tus puntos caducaron por inactividad (6 meses sin rentas — reglas del programa §6); tus proximas rentas vuelven a acumular desde cero' });
    }
    const countableOrders = earned.orders.length;
    const avgTicketCents = countableOrders ? Math.round(earned.revenue_cents / countableOrders) : 0;
    const catalog = rewardsCatalogFor(avgTicketCents);
    const reward = catalog.find(r => r.id === rewardId);
    if (!reward) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
    // saldo REAL: propias + ganadas por atribución − cedidas − canjeadas
    const available = Math.max(0, earned.points_earned
      + ((ledger.atribuciones_ganadas || []).reduce((s, x) => s + (Number(x.puntos) || 0), 0))
      - ((ledger.atribuciones_cedidas || []).reduce((s, x) => s + (Number(x.puntos) || 0), 0))
      - ledger.redeemed_points);
    if (available < reward.points) {
      return res.status(409).json({ ok: false, error: 'puntos insuficientes', points_available: available });
    }

    // Tope de canjes por miembro/día: el portal del cliente no tiene login
    // (entra con su email), así que sin esto un tercero podría quemarle los
    // puntos a un miembro creando canjes en cadena (auditoría 25-jul-2026).
    if (!rewardsRedeemAllowed(customer.id)) {
      return res.status(429).json({ ok: false, error: 'ya hiciste varios canjes hoy; escríbenos por WhatsApp para ayudarte' });
    }

    const credito = rewardsFormatMXN(reward.credito_cents);
    const folio = 'RWD-' + Date.now().toString(36).toUpperCase() + '-' +
      Math.random().toString(36).slice(2, 6).toUpperCase();
    const wrote = await rewardsLedgerWrite({
      tipo: 'canje',
      folio: folio,
      fecha: new Date().toISOString(),
      customer_id: customer.id,
      email: email,
      nombre: rewardsCleanName((customer.attributes || {}).name),
      reward_id: reward.id,
      reward_name: reward.name,               // ej. "Crédito de $700 en tu próxima renta"
      puntos: reward.points,
      descuento_pct: 0,                       // ya no hay %: el premio es crédito
      credito_mxn: reward.credito_cents / 100, // número en pesos (el .gs lo ignora sin romper)
      estado: 'pendiente',
      ip: rewardsClientIp(req),
      ua: rewardsClientUa(req)
    });
    if (!wrote) return res.status(502).json({ ok: false, error: 'no se pudo registrar el canje, intenta de nuevo' });

    console.log('[rewards] canje ' + folio + ' ' + email + ' -' + reward.points + ' pts (crédito ' + credito + ')');
    return res.json({
      ok: true,
      folio: folio,
      reward: reward,
      points_available: available - reward.points,
      // el Ledger esta configurado (503 arriba si no) y el .gs manda el correo
      // de confirmacion al registrar la fila del canje
      email_confirmacion: true,
      instrucciones: 'Presenta el folio ' + folio + ' al confirmar tu próxima renta para aplicar tu descuento de ' +
        credito + '. Aplica en rentas de al menos el doble del descuento; el folio no da cambio.'
    });
  } catch (e) {
    console.error('[rewards] redeem error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error procesando el canje, intenta de nuevo' });
  }
});

// ── POST /rewards/scan  {code, pin?, order_number?, staff_name?} ──
// Resuelve el QR de miembro contra un indice qr->customer construido paginando
// /customers (cache 12h; se reconstruye si el codigo no aparece).
let rewardsQrIndex = null;       // Map code -> {id, name, email, number}
let rewardsQrIndexAt = 0;
let rewardsQrAmbiguous = {};     // codigos con colision (no resolubles)

async function rewardsBuildQrIndex() {
  const idx = new Map();
  const ambiguous = {};
  const seen = new Set(); // dedupe por id, por si acaso
  for (let page = 1; page <= 60; page++) {
    // sort=number (unico y estable): con sort=created_at los empates hacen que la
    // paginacion REPITA ~500 clientes y OMITA otros ~500 (verificado 22-jul-2026)
    const d = await booqableGet('/customers?page[size]=100&page[number]=' + page + '&sort=number');
    const data = d.data || [];
    for (const c of data) {
      if (seen.has(c.id)) continue;
      seen.add(c.id);
      if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(c.id)) continue; // socios/internos fuera
      const a = c.attributes || {};
      const code = rewardsQrCode(c.id);
      if (idx.has(code)) {
        // colision REAL solo si son clientes distintos
        if (idx.get(code).id !== c.id) {
          ambiguous[code] = true;
          console.error('[rewards] COLISION de QR ' + code + ': ' + idx.get(code).id + ' vs ' + c.id);
        }
        continue;
      }
      idx.set(code, { id: c.id, name: rewardsCleanName(a.name), email: a.email || '', number: a.number });
    }
    if (data.length < 100) break;
  }
  rewardsQrIndex = idx;
  rewardsQrAmbiguous = ambiguous;
  rewardsQrIndexAt = Date.now();
  console.log('[rewards] indice QR: ' + idx.size + ' miembros, ' + Object.keys(ambiguous).length + ' colisiones');
}

// ── Identificar al miembro en el mostrador: QR, correo o celular (v8.38) ──
// Hasta v8.37 el mostrador SOLO aceptaba el código del QR. En la práctica el
// cliente casi nunca trae el portal abierto y el staff se quedaba sin forma de
// encontrarlo (lo preguntaron Alfredo el 20-ago y Barush el 25-ago-2026).
// Ahora un mismo campo acepta las tres formas y el server decide cuál es por la
// FORMA de lo capturado — el staff no tiene que elegir modo:
//   FLM-XX-YYYY-XNXN (pegado, con guiones o en minúsculas) → código del QR
//   cualquier cosa con @                                    → correo
//   10 dígitos o más                                        → celular (WhatsApp)
// El celular puede tocar 2+ fichas (23 números están compartidos en Booqable,
// casi siempre duplicados de la misma persona): ahí NO se adivina — se regresan
// los candidatos con 409 para que el humano del mostrador elija cuál es.
function rewardsNormCodigoQr(raw) {
  let s = String(raw || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (s.indexOf('FLM') === 0) s = s.slice(3);
  return /^[A-Z]{2}\d{4}[A-Z]\d[A-Z]\d$/.test(s)
    ? 'FLM-' + s.slice(0, 2) + '-' + s.slice(2, 6) + '-' + s.slice(6)
    : '';
}

async function rewardsResolverQr(code) {
  const stale = !rewardsQrIndex || (Date.now() - rewardsQrIndexAt) > 12 * 3600 * 1000;
  if (stale) await rewardsBuildQrIndex();
  let hit = rewardsQrIndex.get(code);
  if (!hit && !stale) { await rewardsBuildQrIndex(); hit = rewardsQrIndex.get(code); }
  if (rewardsQrAmbiguous[code]) {
    return { error: { status: 409, error: 'ese codigo lo comparten dos fichas: busca al cliente por su correo o su celular' } };
  }
  if (!hit) return { error: { status: 404, error: 'codigo no encontrado' } };
  return { customerId: hit.id, via: 'qr' };
}

// TODAS las fichas con ese correo, no la primera. En Booqable el correo tampoco
// es único: medido el 25-ago-2026, 131 celulares están repetidos en 2+ fichas y
// los duplicados suelen compartir también el correo (ficha buena + ficha
// "NOusar…NOusar"). Devolver la primera que conteste es cómo un crédito termina
// en el expediente equivocado, así que aquí se devuelven todas y quien decide es
// el humano del mostrador.
// filter[email] es exacto: si la ficha dice "Juan@Gmail.com" y el staff escribe
// minúsculas, no aparece — por eso el respaldo con filter[q], que CONFIRMA
// igualdad exacta en minúsculas (mismo candado que el post-filtro del teléfono).
async function rewardsBuscarPorEmail(email) {
  const d = await booqableGet('/customers?filter[email]=' + encodeURIComponent(email) + '&page[size]=25');
  let hits = (d.data || []).filter(c =>
    String(((c.attributes || {}).email) || '').trim().toLowerCase() === email);
  if (!hits.length) {
    const q = await booqableGet('/customers?filter[q]=' + encodeURIComponent(email) + '&page[size]=25');
    hits = (q.data || []).filter(c =>
      String(((c.attributes || {}).email) || '').trim().toLowerCase() === email);
  }
  hits = hits.filter(c => !REWARDS_EXCLUDED_CUSTOMER_IDS.has(c.id));
  // Las fichas archivadas solo se usan si no hay ninguna viva: así el mostrador
  // sigue encontrando al cliente viejo, pero nunca por encima de su ficha buena.
  const vivas = hits.filter(c => !((c.attributes || {}).archived));
  const usar = vivas.length ? vivas : hits;
  usar.sort((x, y) => ((y.attributes || {}).order_count || 0) - ((x.attributes || {}).order_count || 0));
  return usar;
}

function rewardsCandidatoResumen(c) {
  const a = c.attributes || {};
  return {
    customer_id: c.id,
    nombre: rewardsCleanName(a.name),
    email: a.email || '',
    rentas: a.order_count || 0
  };
}

// opts.mostrador: quien pregunta es un empleado ya autenticado (Google/PIN).
//   Solo entonces se permite buscar POR CELULAR, ver la lista de fichas
//   candidatas y mandar un customer_id explícito. Sin esa bandera (el cliente
//   en self-service) el celular no es una llave de búsqueda: sería un
//   directorio de nombres y correos ajenos a un tecleo de distancia.
// opts.telefonoMasActivo: ante duplicados por celular toma la ficha con más
//   rentas en vez de preguntar (solo cupones: el cupón vive en el TELÉFONO, y
//   las fichas duplicadas son la misma persona).
async function rewardsResolverMiembro(body, opts) {
  opts = opts || {};
  body = body || {};

  if (opts.mostrador && String(body.customer_id || '').trim()) {
    const c = await rewardsCustomerById(String(body.customer_id).trim());
    if (!c) return { error: { status: 404, error: 'esa ficha ya no existe en Booqable' } };
    return { customerId: c.id, via: 'ficha' };
  }

  // `code` se sigue leyendo como texto libre a propósito: así el mostrador que
  // ya está instalado (y el espejo de Bluehost, que sincroniza hasta las 3am)
  // aceptan correo y celular sin esperar a que se actualice el HTML.
  const libre = String(body.q || body.code || '').trim();
  const qr = rewardsNormCodigoQr(libre);
  if (qr) return rewardsResolverQr(qr);

  const correo = libre.indexOf('@') !== -1
    ? libre.toLowerCase()
    : String(body.email || '').trim().toLowerCase();
  if (correo && correo.indexOf('@') !== -1) {
    const cs = await rewardsBuscarPorEmail(correo);
    if (!cs.length) return { error: { status: 404, error: 'no hay ninguna cuenta con ese correo' } };
    // Igual que con el celular: si el correo toca 2+ fichas, el mostrador elige.
    // En self-service se conserva la ficha con más rentas (comportamiento que ya
    // tenía /atribuir): el candado de sesión valida después que sea suya.
    if (cs.length > 1 && opts.mostrador) {
      return { error: {
        status: 409,
        error: 'ese correo aparece en ' + cs.length + ' fichas: dime cuál cliente es',
        candidatos: cs.slice(0, 6).map(rewardsCandidatoResumen)
      } };
    }
    return { customerId: cs[0].id, via: 'correo' };
  }

  const p10 = opts.mostrador ? rewardsPhone10(/\d/.test(libre) ? libre : body.telefono) : '';
  if (p10) {
    const cands = await rewardsCustomersByPhone(p10);
    if (!cands.length) {
      return { error: { status: 404, error: 'no hay ninguna cuenta con ese celular. Si es cliente nuevo, dalo de alta en Booqable con su celular y sus puntos empiezan a contar solos' } };
    }
    cands.sort((x, y) => ((y.attributes || {}).order_count || 0) - ((x.attributes || {}).order_count || 0));
    if (cands.length > 1 && !opts.telefonoMasActivo) {
      // Aquí solo llega el mostrador (el celular ni se calcula sin esa bandera),
      // por eso los candidatos pueden traer nombre y correo: los está viendo un
      // empleado identificado que tiene que elegir, no un cliente cualquiera.
      return { error: {
        status: 409,
        error: 'ese celular aparece en ' + cands.length + ' fichas: dime cuál cliente es',
        candidatos: cands.slice(0, 6).map(rewardsCandidatoResumen)
      } };
    }
    return { customerId: cands[0].id, via: 'celular' };
  }

  return { error: { status: 400, error: opts.mostrador
    ? 'escanea el QR del cliente, o escribe su correo o su celular a 10 digitos'
    : 'manda el codigo del QR o el correo de la cuenta' } };
}

function rewardsResolverFallo(res, r) {
  if (!r || !r.error) return false;
  const cuerpo = { ok: false, error: r.error.error };
  if (r.error.candidatos) cuerpo.candidatos = r.error.candidatos;
  res.status(r.error.status).json(cuerpo);
  return true;
}

app.post('/rewards/scan', async (req, res) => {
  const body = req.body || {};
  const staffScan = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffScan)) return;
  try {
    // QR, correo o celular — lo resuelve la FORMA de lo capturado (v8.38)
    const r = await rewardsResolverMiembro(body, { mostrador: true });
    if (rewardsResolverFallo(res, r)) return;
    // El índice del QR ya excluye a socios/internos; correo y celular llegan
    // directo a Booqable, así que la exclusión se revalida aquí.
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(r.customerId)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }

    // resumen completo del miembro (puntos en vivo)
    const cd = await booqableGet('/customers/' + r.customerId);
    const customer = cd.data;
    const out = await rewardsBuildMember(customer);
    const codeQr = rewardsQrCode(r.customerId);

    const logged = await rewardsLedgerWrite({
      tipo: 'scan',
      fecha: new Date().toISOString(),
      // en el Ledger siempre queda el código del miembro, lo hayan
      // identificado por QR, por correo o por celular
      code: codeQr,
      customer_id: r.customerId,
      nombre: out.member.name,
      email: out.member.email,
      order_number: body.order_number || '',
      // identidad VERIFICADA (Google/PIN); nunca el staff_name que mande el
      // navegador — ese campo ya no es identidad (auditoría 25-jul)
      staff_name: staffScan,
      ip: rewardsClientIp(req),
      ua: rewardsClientUa(req)
    });

    console.log('[rewards] scan por ' + r.via + ' ' + codeQr + ' -> ' + out.member.name + ' (logged=' + logged + ')');
    return res.json(Object.assign({
      ok: true, logged: logged, via: r.via, customer_id: r.customerId, qr_code: codeQr
    }, out));
  } catch (e) {
    console.error('[rewards] scan error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error resolviendo el codigo, intenta de nuevo' });
  }
});

// ── POST /rewards/pagar  {code|email, reward_id, order_number, staff_name?, pin?} ──
// "Pagar con puntos" en una escaneada (aprobado por Daniel 24-jul-2026): el staff
// escanea el QR del miembro (o captura su email), elige el crédito de su catálogo
// calibrado y captura el # de orden; el server APLICA el descuento en Booqable
// (línea negativa CON IVA — el cliente ve el crédito exacto en pesos) y registra
// el canje ya 'aplicado' en el Ledger. El folio queda solo como registro interno.
// Candados: PIN de staff, saldo y catálogo recalculados en vivo, orden ≥ 2× el
// crédito, 1 crédito Rewards por orden, orden cancelada rechazada.
app.post('/rewards/pagar', async (req, res) => {
  const body = req.body || {};
  const staffPagar = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffPagar)) return;
  const rewardId = parseInt(body.reward_id, 10);
  const orderNumber = parseInt(body.order_number, 10);
  if (!Number.isFinite(rewardId)) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
  if (!Number.isFinite(orderNumber)) return res.status(400).json({ ok: false, error: 'order_number invalido' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'pagos con puntos deshabilitados (Ledger no configurado)' });
  try {
    // 1) resolver al miembro: QR, correo o celular (v8.38)
    const rp = await rewardsResolverMiembro(body, { mostrador: true });
    if (rewardsResolverFallo(res, rp)) return;
    const customerId = rp.customerId;
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customerId)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }
    const cd = await booqableGet('/customers/' + customerId);
    const customer = cd.data;

    // 2) saldo + catálogo calibrado EN VIVO (no confiar en el cliente)
    const earned = await rewardsComputeEarned(customerId);
    const ledger = await rewardsLedgerSummary(customerId, ((customer || {}).attributes || {}).email);
    if (!ledger) return res.status(503).json({ ok: false, error: 'no se pudo leer el Ledger, intenta mas tarde' });
    const candadoPagar = rewardsCandadoCanje(ledger, earned.orders);
    if (candadoPagar) return res.status(candadoPagar.status).json({ ok: false, error: candadoPagar.error });
    if (earned.vigencia && earned.vigencia.caducado) {
      return res.status(409).json({ ok: false, error: 'los puntos de esta cuenta caducaron por inactividad (6 meses sin rentas); sus proximas rentas vuelven a acumular desde cero' });
    }
    const countableOrders = earned.orders.length;
    const avgTicketCents = countableOrders ? Math.round(earned.revenue_cents / countableOrders) : 0;
    const reward = rewardsCatalogFor(avgTicketCents).find(r => r.id === rewardId);
    if (!reward) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
    // saldo REAL: propias + ganadas por atribución − cedidas − canjeadas
    const available = Math.max(0, earned.points_earned
      + ((ledger.atribuciones_ganadas || []).reduce((s, x) => s + (Number(x.puntos) || 0), 0))
      - ((ledger.atribuciones_cedidas || []).reduce((s, x) => s + (Number(x.puntos) || 0), 0))
      - ledger.redeemed_points);
    if (available < reward.points) {
      return res.status(409).json({ ok: false, error: 'puntos insuficientes', points_available: available });
    }

    // 3) la orden destino: existe, no cancelada, regla 2x, sin crédito previo
    if (REWARDS_EXCLUDED_ORDER_NUMBERS.has(orderNumber)) {
      return res.status(409).json({ ok: false, error: 'los descuentos Rewards aplican solo en rentas, no en ventas de equipo' });
    }
    const od = await booqableGet('/orders?filter[number]=' + orderNumber + '&page[size]=2');
    const order = (od.data || [])[0];
    if (!order) return res.status(404).json({ ok: false, error: 'no existe la orden #' + orderNumber });
    const oa = order.attributes || {};
    if (oa.status === 'canceled') {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' esta cancelada' });
    }
    // Términos §4: el crédito aplica solo en rentas NUEVAS, antes de facturar y
    // pagar — no en rentas en curso, ya devueltas o ya pagadas. El documento lo
    // prometía y el server solo rechazaba canceladas (review 31-jul-2026).
    if (oa.status === 'started' || oa.status === 'stopped') {
      return res.status(409).json({
        ok: false,
        error: 'la orden #' + orderNumber + (oa.status === 'started' ? ' ya esta en curso' : ' ya termino') +
          ': el descuento aplica solo en rentas nuevas, antes de facturar'
      });
    }
    if (oa.payment_status === 'paid') {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' ya esta pagada: el descuento se aplica antes del pago' });
    }
    const totalWithTax = oa.grand_total_with_tax_in_cents || 0;
    if (totalWithTax < reward.credito_cents * 2) {
      return res.status(409).json({
        ok: false,
        error: 'la orden debe ser de al menos ' + rewardsFormatMXN(reward.credito_cents * 2) +
          ' (2x el descuento de ' + rewardsFormatMXN(reward.credito_cents) + '); total actual con IVA: ' + rewardsFormatMXN(totalWithTax)
      });
    }
    const ld = await booqableGet('/lines?filter[order_id]=' + order.id + '&page[size]=100');
    const lineasOrden = (ld.data || []).filter(l => !((l.attributes || {}).archived));
    const yaTiene = lineasOrden.some(l =>
      String((l.attributes || {}).title || '').toLowerCase().indexOf('filmorent rewards') === 0);
    if (yaTiene) {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' ya tiene un descuento Rewards aplicado' });
    }
    // regla de Daniel 24-jul-2026: los puntos NO aplican a ventas de equipo —
    // si la orden trae una línea de venta (prefijo "VENTA"), se rechaza
    const esVenta = lineasOrden.some(l => {
      const t = String((l.attributes || {}).title || '').toLowerCase().trim();
      return t.indexOf('venta ') === 0 || t.indexOf('venta-') === 0 || t.indexOf('venta:') === 0;
    });
    if (esVenta) {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' incluye venta de equipo: los descuentos Rewards aplican solo en rentas' });
    }

    // 4) aplicar el descuento en Booqable
    const folio = 'RWD-' + Date.now().toString(36).toUpperCase() + '-' +
      Math.random().toString(36).slice(2, 6).toUpperCase();
    const credito = rewardsFormatMXN(reward.credito_cents);
    await booqableWrite('POST', '/lines', {
      data: {
        type: 'lines',
        attributes: {
          owner_id: order.id,
          owner_type: 'orders',
          title: 'Filmorent Rewards - descuento ' + credito + ' (' + folio + ')',
          quantity: 1,
          price_each_in_cents: -reward.credito_cents
        }
      }
    });

    // 5) registrar en el Ledger: fila de canje (manda el email al cliente) y
    //    de inmediato marcarla 'aplicado' con la orden y el staff. Si el Ledger
    //    falla DESPUÉS de aplicar la línea, avisar para registro manual.
    const staffName = staffPagar;   // identidad verificada, no el body
    const wrote = await rewardsLedgerWrite({
      tipo: 'canje',
      folio: folio,
      fecha: new Date().toISOString(),
      customer_id: customerId,
      email: (customer.attributes || {}).email || '',
      nombre: rewardsCleanName((customer.attributes || {}).name),
      reward_id: reward.id,
      reward_name: reward.name,
      puntos: reward.points,
      descuento_pct: 0,
      credito_mxn: reward.credito_cents / 100,
      estado: 'pendiente',
      ip: rewardsClientIp(req),
      ua: rewardsClientUa(req)
    });
    let aplicadoEnLedger = false;
    if (wrote) {
      aplicadoEnLedger = await rewardsLedgerWrite({
        tipo: 'aplicar',
        folio: folio,
        order_number: String(orderNumber),
        staff_name: staffName || 'auto (pagar con puntos)'
      });
    }

    // total nuevo de la orden (recalculado por Booqable)
    let nuevoTotal = null;
    try {
      const od2 = await booqableGet('/orders/' + order.id);
      nuevoTotal = ((od2.data || {}).attributes || {}).grand_total_with_tax_in_cents;
    } catch (e2) { /* solo informativo */ }

    console.log('[rewards] pagar ' + folio + ' ' + (customer.attributes || {}).email + ' -' + reward.points +
      ' pts -> orden #' + orderNumber + ' (' + credito + ', ledger=' + (wrote ? (aplicadoEnLedger ? 'aplicado' : 'pendiente') : 'FALLO') + ')');
    return res.json({
      ok: true,
      folio: folio,
      credito_mxn: reward.credito_cents / 100,
      puntos_usados: reward.points,
      points_restantes: available - reward.points,
      order_number: orderNumber,
      nuevo_total_mxn: nuevoTotal === null ? null : nuevoTotal / 100,
      ledger_ok: !!wrote,
      advertencia: wrote ? null : 'el descuento SE APLICO en Booqable pero el Ledger no respondio: anota el folio ' + folio + ' manualmente en el Sheet'
    });
  } catch (e) {
    console.error('[rewards] pagar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error aplicando el descuento, intenta de nuevo' });
  }
});

// ── Cupones / crédito promocional (19-ago-2026) ─────────────
// Un cupón es crédito en PESOS con vencimiento y condición propios, distinto de
// los puntos. Se emite por campaña (estudio, estreno FX5, win-back…), vive en el
// portal del cliente, y se aplica en mostrador como el pago con puntos — pero sin
// gastar puntos. Nace del hallazgo de la auditoría: un descuento a mano en
// Booqable no descuenta nada ni se puede medir; un cupón sí.

// slug corto y seguro para meterlo en el id del cupón
function rewardsSlug(s) {
  return String(s || 'promo').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 24) || 'promo';
}

// POST /rewards/cupon/emitir  — emite un cupón para un cliente.
// Auth: staff autenticado O la clave de administración (k === HITOS_KEY), para
// poder emitir campañas de forma programática desde una sesión de operación.
// Body: { email|code, monto_cents, campana, condicion?, vence? (YYYY-MM-DD), notas? }
app.post('/rewards/cupon/emitir', async (req, res) => {
  const body = req.body || {};
  const adminKey = process.env.REWARDS_HITOS_KEY && String(body.k || '') === process.env.REWARDS_HITOS_KEY;
  let quienEmite = 'admin (clave)';
  if (!adminKey) {
    const staff = await rewardsStaffFrom(req);
    if (rewardsStaffDenied(res, staff)) return;
    quienEmite = staff;
  }
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'cupones deshabilitados (Ledger no configurado)' });
  const montoCents = parseInt(body.monto_cents, 10);
  if (!Number.isFinite(montoCents) || montoCents <= 0) return res.status(400).json({ ok: false, error: 'monto_cents invalido' });
  const campana = String(body.campana || '').trim();
  if (!campana) return res.status(400).json({ ok: false, error: 'falta campana' });
  const vence = String(body.vence || '').trim();
  if (vence && !/^\d{4}-\d{2}-\d{2}$/.test(vence)) return res.status(400).json({ ok: false, error: 'vence debe ser YYYY-MM-DD' });
  if (vence && vence < rewardsHoyMty()) return res.status(400).json({ ok: false, error: 'la fecha de vencimiento ya paso' });
  try {
    // resolver al cliente por email, por QR o por TELÉFONO (v8.36). El teléfono
    // permite emitirle a un lead que NUNCA ha rentado: pidió equipo, no había,
    // se fue — y por eso mismo no tiene cuenta en Booqable. El cupón nace
    // amarrado a su celular y cupon_usar lo liga a la cuenta cuando rente.
    const code = String(body.code || '').trim().toUpperCase();
    const email = String(body.email || '').trim().toLowerCase();
    const tel10 = rewardsPhone10(body.telefono);
    let customer = null;
    let soloTelefono = false;
    if (email && email.indexOf('@') !== -1) {
      customer = await rewardsFindCustomer(email);
      if (!customer) return res.status(404).json({ ok: false, error: 'no existe cuenta con ese email' });
    } else if (code) {
      const stale = !rewardsQrIndex || (Date.now() - rewardsQrIndexAt) > 12 * 3600 * 1000;
      if (stale) await rewardsBuildQrIndex();
      const hit = rewardsQrIndex.get(code);
      if (rewardsQrAmbiguous[code]) return res.status(409).json({ ok: false, error: 'codigo ambiguo, usa el email' });
      if (hit) { const cd = await booqableGet('/customers/' + hit.id); customer = cd.data; }
      if (!customer) return res.status(404).json({ ok: false, error: 'no existe cuenta con ese codigo' });
    } else if (tel10) {
      // si el teléfono SÍ tiene ficha, mejor amarrar a la cuenta desde ya
      const porTel = await rewardsCustomersByPhone(tel10);
      if (porTel.length) {
        porTel.sort((x, y) => ((y.attributes || {}).order_count || 0) - ((x.attributes || {}).order_count || 0));
        customer = porTel[0];
      } else {
        soloTelefono = true;
        if (!String(body.nombre || '').trim()) {
          return res.status(400).json({ ok: false, error: 'para emitir por telefono sin cuenta, manda tambien el nombre' });
        }
      }
    } else {
      return res.status(400).json({ ok: false, error: 'manda email, code o telefono del cliente' });
    }
    if (customer && REWARDS_EXCLUDED_CUSTOMER_IDS.has(customer.id)) {
      return res.status(403).json({ ok: false, error: 'esa cuenta no participa en Filmorent Rewards' });
    }
    const cuponId = 'CUP-' + rewardsSlug(campana).toUpperCase() + '-' +
      Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 5).toUpperCase();
    const nombreCupon = customer
      ? rewardsCleanName((customer.attributes || {}).name)
      : String(body.nombre || '').trim().slice(0, 80);
    const wrote = await rewardsLedgerWrite({
      tipo: 'cupon_emitir',
      cupon_id: cuponId,
      fecha: new Date().toISOString(),
      customer_id: customer ? customer.id : '',
      email: customer ? ((customer.attributes || {}).email || '') : '',
      nombre: nombreCupon,
      monto_cents: montoCents,
      campana: campana,
      condicion: String(body.condicion || '').trim(),
      vence: vence,
      emitido_por: quienEmite,
      notas: String(body.notas || '').trim(),
      telefono: tel10
    });
    if (!wrote) return res.status(502).json({ ok: false, error: 'no se pudo escribir el cupon al Ledger' });
    console.log('[rewards] cupon emitido ' + cuponId + ' ' + rewardsFormatMXN(montoCents) + ' -> ' +
      (customer ? (((customer.attributes || {}).email) || customer.id) : ('tel ...' + tel10.slice(-4) + ' (sin cuenta aun)')) +
      ' (' + campana + ', vence ' + (vence || 'sin fecha') + ')');
    return res.json({
      ok: true, cupon_id: cuponId, monto_mxn: montoCents / 100, campana: campana,
      condicion: String(body.condicion || '').trim() || null, vence: vence || null,
      cliente: nombreCupon, sin_cuenta: soloTelefono || undefined
    });
  } catch (e) {
    console.error('[rewards] cupon emitir error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error emitiendo el cupon' });
  }
});

// POST /rewards/cupon/aplicar  — aplica un cupón a una orden (mostrador).
// Auth: staff (mueve dinero en Booqable). Body: { email|code, cupon_id, order_number }.
app.post('/rewards/cupon/aplicar', async (req, res) => {
  const body = req.body || {};
  const staffPagar = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffPagar)) return;
  const cuponId = String(body.cupon_id || '').trim();
  const orderNumber = parseInt(body.order_number, 10);
  if (!cuponId) return res.status(400).json({ ok: false, error: 'falta cupon_id' });
  if (!Number.isFinite(orderNumber)) return res.status(400).json({ ok: false, error: 'order_number invalido' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'cupones deshabilitados (Ledger no configurado)' });
  try {
    // 1) el miembro: QR, correo o celular. telefonoMasActivo porque el cupón vive
    // en el TELÉFONO (v8.36, lead sin cuenta): si ese celular tiene fichas
    // duplicadas son la misma persona, y se toma la que más ha rentado.
    const rc = await rewardsResolverMiembro(body, { mostrador: true, telefonoMasActivo: true });
    if (rewardsResolverFallo(res, rc)) return;
    const customerId = rc.customerId;
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customerId)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }
    const cd = await booqableGet('/customers/' + customerId);
    const customer = cd.data;

    // 2) el cupón: tiene que ser suyo, vigente y no usado. El server NO confía en
    //    el cliente: relee el Ledger y valida contra los cupones vigentes reales.
    //    El candado de adeudo usa las órdenes REALES (igual que /pagar), para que
    //    el proxy automático de Booqable (stopped>30d sin pagar) también cuente.
    //    El teléfono de la ficha entra al lookup para que un cupón emitido por
    //    celular (antes de que existiera la cuenta) aparezca como suyo.
    const earnedCup = await rewardsComputeEarned(customerId);
    const pTelAp = ((customer || {}).attributes || {}).properties || {};
    const telAp = rewardsPhone10(pTelAp.phone || pTelAp.phone_2 || ((customer || {}).attributes || {}).phone || body.telefono);
    const ledger = await rewardsLedgerSummary(customerId, ((customer || {}).attributes || {}).email, telAp);
    if (!ledger) return res.status(503).json({ ok: false, error: 'no se pudo leer el Ledger, intenta mas tarde' });
    const candadoCup = rewardsCandadoCanje(ledger, earnedCup.orders);
    if (candadoCup) return res.status(candadoCup.status).json({ ok: false, error: candadoCup.error });
    const vigentes = rewardsCuponesVigentes(ledger.cupones);
    const cupon = vigentes.find(c => c.cupon_id === cuponId);
    if (!cupon) {
      // distinguir "no es tuyo/no existe" de "ya usado/vencido" para un mensaje claro
      const crudo = (ledger.cupones || []).find(c => c.cupon_id === cuponId);
      if (crudo && String(crudo.estado).toLowerCase() === 'usado') return res.status(409).json({ ok: false, error: 'ese cupon ya se uso' });
      if (crudo) return res.status(409).json({ ok: false, error: 'ese cupon ya no esta vigente (vencido o cancelado)' });
      return res.status(404).json({ ok: false, error: 'ese cupon no existe o no es de esta cuenta' });
    }

    // 3) la orden destino — mismas reglas que el pago con puntos (§4)
    if (REWARDS_EXCLUDED_ORDER_NUMBERS.has(orderNumber)) {
      return res.status(409).json({ ok: false, error: 'los descuentos Rewards aplican solo en rentas, no en ventas de equipo' });
    }
    const od = await booqableGet('/orders?filter[number]=' + orderNumber + '&page[size]=2');
    const order = (od.data || [])[0];
    if (!order) return res.status(404).json({ ok: false, error: 'no existe la orden #' + orderNumber });
    const oa = order.attributes || {};
    if (oa.status === 'canceled') return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' esta cancelada' });
    if (oa.status === 'started' || oa.status === 'stopped') {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + (oa.status === 'started' ? ' ya esta en curso' : ' ya termino') + ': el descuento aplica solo en rentas nuevas, antes de facturar' });
    }
    if (oa.payment_status === 'paid') return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' ya esta pagada: el descuento se aplica antes del pago' });
    const totalWithTax = oa.grand_total_with_tax_in_cents || 0;
    if (totalWithTax < cupon.monto_cents * 2) {
      return res.status(409).json({ ok: false, error: 'la orden debe ser de al menos ' + rewardsFormatMXN(cupon.monto_cents * 2) + ' (2x el cupon de ' + rewardsFormatMXN(cupon.monto_cents) + '); total actual con IVA: ' + rewardsFormatMXN(totalWithTax) });
    }
    const ld = await booqableGet('/lines?filter[order_id]=' + order.id + '&page[size]=100');
    const lineasOrden = (ld.data || []).filter(l => !((l.attributes || {}).archived));
    const yaTiene = lineasOrden.some(l => String((l.attributes || {}).title || '').toLowerCase().indexOf('filmorent rewards') === 0);
    if (yaTiene) return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' ya tiene un descuento Rewards aplicado' });
    const esVenta = lineasOrden.some(l => {
      const t = String((l.attributes || {}).title || '').toLowerCase().trim();
      return t.indexOf('venta ') === 0 || t.indexOf('venta-') === 0 || t.indexOf('venta:') === 0;
    });
    if (esVenta) return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' incluye venta de equipo: los descuentos Rewards aplican solo en rentas' });
    // condición del cupón: si dice 'estudio', la orden debe traer una línea de estudio
    const condicion = String(cupon.condicion || '').toLowerCase();
    if (condicion.indexOf('estudio') !== -1) {
      const tieneEstudio = lineasOrden.some(l => {
        const t = String((l.attributes || {}).title || '').toLowerCase();
        return t.indexOf('estudio') !== -1 && t.indexOf('encargado') === -1;
      });
      if (!tieneEstudio) return res.status(409).json({ ok: false, error: 'este cupon es solo para renta de estudio: la orden #' + orderNumber + ' no incluye estudio' });
    }

    // 4) CLAIM-FIRST: reclamar el cupón en el Ledger ANTES de tocar Booqable.
    //    El LockService del .gs hace el marcado atómico; solo si updated===true
    //    (pasó de 'activo' a 'usado' en ESTA llamada) seguimos. Así un doble
    //    clic o dos cajas simultáneas NO producen dos descuentos: la 2ª ve
    //    updated:false y se rechaza sin mover dinero. Si el Ledger no confirma,
    //    NADA se aplica en Booqable (revisión adversarial 19-ago-2026).
    const claim = await rewardsLedgerCall({
      tipo: 'cupon_usar', cupon_id: cupon.cupon_id,
      order_number: String(orderNumber), staff_name: staffPagar || 'mostrador',
      // amarre v8.36: si el cupón nació con teléfono (sin cuenta), al usarse se
      // le escribe la cuenta recién creada — el crédito queda en su historial
      customer_id: customerId,
      email: ((customer || {}).attributes || {}).email || ''
    });
    if (!claim || claim.ok === false) {
      return res.status(502).json({ ok: false, error: 'no se pudo registrar el cupon, intenta de nuevo (no se aplico ningun descuento)' });
    }
    if (claim.updated === false) {
      return res.status(409).json({ ok: false, error: 'ese cupon ya se uso' });
    }

    // 5) ya reclamado: aplicar el descuento en Booqable. Si esto falla, el cupón
    //    queda 'usado' sin descuento (recuperable a mano) — preferible a fugar dinero.
    const etiqueta = 'Filmorent Rewards - promo ' + rewardsFormatMXN(cupon.monto_cents) + ' (' + cupon.cupon_id + ')';
    try {
      await booqableWrite('POST', '/lines', {
        data: { type: 'lines', attributes: {
          owner_id: order.id, owner_type: 'orders', title: etiqueta,
          quantity: 1, price_each_in_cents: -cupon.monto_cents
        } }
      });
    } catch (eBq) {
      console.error('[rewards] cupon ' + cupon.cupon_id + ' RECLAMADO pero fallo Booqable: ' + eBq.message);
      return res.status(502).json({
        ok: false,
        error: 'el cupon quedo registrado pero el descuento no se aplico en Booqable (' + cupon.cupon_id +
          '): avisa con este folio para aplicarlo a mano o liberarlo'
      });
    }

    let nuevoTotal = null;
    try { const od2 = await booqableGet('/orders/' + order.id); nuevoTotal = ((od2.data || {}).attributes || {}).grand_total_with_tax_in_cents; } catch (e2) { /* informativo */ }
    console.log('[rewards] cupon aplicado ' + cupon.cupon_id + ' ' + rewardsFormatMXN(cupon.monto_cents) +
      ' -> orden #' + orderNumber);
    return res.json({
      ok: true, cupon_id: cupon.cupon_id, monto_mxn: cupon.monto_cents / 100,
      order_number: orderNumber, nuevo_total_mxn: nuevoTotal === null ? null : nuevoTotal / 100,
      ledger_ok: true
    });
  } catch (e) {
    console.error('[rewards] cupon aplicar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error aplicando el cupon, intenta de nuevo' });
  }
});

// ── POST /rewards/hitos-diarios?key=... ─────────────────────
// Motor de avisos por HITOS (decisión de Daniel 1-ago-2026: WhatsApp solo cuando
// pasa algo que le importa al cliente — cruzó un escalón de canje o subió de
// nivel; NUNCA recibo por cada renta, le preocupa la intromisión). Lo dispara
// un trigger diario del Apps Script del Ledger (cuenta info@). Idempotente vía
// tab Notificaciones (tipo:'notif' con clave única). Mientras no exista la
// plantilla aprobada de Meta corre en DRY-RUN: registra lo que MANDARÍA.
// Encender con env REWARDS_HITOS_LIVE=1 cuando la plantilla esté aprobada.
app.post('/rewards/hitos-diarios', async (req, res) => {
  if (!process.env.REWARDS_HITOS_KEY || req.query.key !== process.env.REWARDS_HITOS_KEY) {
    return res.status(401).json({ ok: false, error: 'key invalida' });
  }
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'Ledger no configurado' });
  const live = process.env.REWARDS_HITOS_LIVE === '1';
  try {
    // "ayer" en horario de Monterrey (UTC-6)
    const ahoraMty = new Date(Date.now() - 6 * 3600 * 1000);
    const ayer = new Date(ahoraMty.getTime() - 24 * 3600 * 1000).toISOString().slice(0, 10);
    const recientes = [];
    for (let page = 1; page <= 3; page++) {
      const od = await booqableGet('/orders?sort=-number&page[size]=100&page[number]=' + page);
      recientes.push(...(od.data || []));
    }
    const deAyer = recientes.filter(o => {
      const a = o.attributes || {};
      if (a.status === 'draft' || a.status === 'concept' || a.status === 'canceled') return false;
      return String(a.starts_at || '').slice(0, 10) === ayer;
    });
    const clientes = [...new Set(deAyer.map(o => (o.attributes || {}).customer_id).filter(Boolean))]
      .filter(cid => !REWARDS_EXCLUDED_CUSTOMER_IDS.has(cid))
      .slice(0, 40);

    const hitos = [];
    for (const cid of clientes) {
      try {
        const earned = await rewardsComputeEarned(cid);
        const ledger = await rewardsLedgerSummary(cid);
        if (!ledger) continue;
        if (ledger.exclusion) continue;   // vetados/adeudos no reciben avisos de hitos
        const ganadas = (ledger.atribuciones_ganadas || []).reduce((s2, x) => s2 + (Number(x.puntos) || 0), 0);
        const cedidas = (ledger.atribuciones_cedidas || []).reduce((s2, x) => s2 + (Number(x.puntos) || 0), 0);
        const dispAhora = Math.max(0, earned.points_earned + ganadas - cedidas - ledger.redeemed_points);
        const ordenesAyer = earned.orders.filter(o => String(o.starts_at || '').slice(0, 10) === ayer);
        const ptsAyer = ordenesAyer.reduce((s2, o) => s2 + (o.points || 0), 0);
        if (ptsAyer <= 0) continue;
        const dispAntes = Math.max(0, dispAhora - ptsAyer);
        const countable = earned.orders.length;
        const avgTicket = countable ? Math.round(earned.revenue_cents / countable) : 0;
        const catalogo = rewardsCatalogFor(avgTicket);
        // el escalón más alto que AYER quedó al alcance y antes no lo estaba
        const cruzados = catalogo.filter(r => r.points > dispAntes && r.points <= dispAhora);
        const cruzado = cruzados.length ? cruzados[cruzados.length - 1] : null;
        // subida de nivel: nivel de hoy vs nivel sin las rentas de ayer
        const tierAhora = rewardsTierFor(earned.revenue_12m_cents, earned.rentas_12m);
        const baseAyer = ordenesAyer.reduce((s2, o) =>
          s2 + Math.max(0, (o.total_cents || 0) - (o.elsepc_excluded_cents || 0)), 0);
        const tierAntes = rewardsTierFor(
          Math.max(0, earned.revenue_12m_cents - baseAyer),
          Math.max(0, (earned.rentas_12m || 0) - ordenesAyer.length));
        const cd = await booqableGet('/customers/' + cid);
        const ca = ((cd.data || {}).attributes || {});
        const pendientes = [];
        if (cruzado) {
          pendientes.push({ tipo: 'escalon', detalle: cruzado.points + 'pts=' + rewardsFormatMXN(cruzado.credito_cents) });
        }
        if (tierAhora.name !== tierAntes.name && (tierAhora.mult || 1) > (tierAntes.mult || 1)) {
          pendientes.push({ tipo: 'nivel', detalle: tierAhora.name });
        }
        for (const h of pendientes) {
          const wrote = await rewardsLedgerWrite({
            tipo: 'notif',
            clave: cid + '|' + h.tipo + '|' + h.detalle,
            fecha: new Date().toISOString(),
            customer_id: cid,
            email: ca.email || '',
            nombre: rewardsCleanName(ca.name),
            tipo_hito: h.tipo,
            detalle: h.detalle,
            estado: live ? 'pendiente-envio' : 'dry-run'
          });
          // el Ledger contesta ok:false si la clave ya existía — no re-avisar
          if (wrote) hitos.push({ cliente: rewardsCleanName(ca.name), email: ca.email || '', tipo: h.tipo, detalle: h.detalle, saldo: dispAhora });
        }
      } catch (eCli) {
        console.error('[rewards] hitos cliente ' + cid + ': ' + eCli.message);
      }
    }
    console.log('[rewards] hitos-diarios ' + ayer + ': ' + deAyer.length + ' ordenes, ' +
      clientes.length + ' clientes, ' + hitos.length + ' hitos nuevos (' + (live ? 'LIVE' : 'dry-run') + ')');
    return res.json({ ok: true, fecha: ayer, ordenes_ayer: deAyer.length,
      clientes_revisados: clientes.length, hitos: hitos, live: live });
  } catch (e) {
    console.error('[rewards] hitos-diarios error: ' + e.message);
    return res.status(502).json({ ok: false, error: e.message });
  }
});

// ── POST /rewards/atribuir ──────────────────────────────────
// "Los puntos de esta renta son de quien la pidió" (28-jul-2026).
// En renta audiovisual el DP/freelance ELIGE el proveedor pero la productora PAGA:
// si no le acreditamos los puntos a quien decide, el programa no influye la decisión.
// La orden en Booqable NO cambia de dueño (factura y cobranza intactas); solo se le
// pone una etiqueta `rw:<código>` para que el equipo lo vea en su propio sistema.
//
// Body: { order_number, code|email, canal?, pin?/id_token? } — o self-service:
//        { order_number, email_solicitante } con la sesión del propio cliente.
// Canales: 'mostrador' (staff autenticado), 'whatsapp' (AI/staff), 'cliente' (self-service).
app.post('/rewards/atribuir', async (req, res) => {
  const body = req.body || {};
  const selfService = String(body.canal || '') === 'cliente';
  let quienRegistra = 'cliente (self-service)';
  // Sesión del cliente para la rama self-service. HASTA v8.32 esta rama NO pedía
  // credencial alguna: como los números de orden son consecutivos y un email no
  // es secreto, cualquiera podía atribuirse los puntos de una renta ajena y
  // canjearlos (auditoría 19-ago-2026). Ahora exige el token OTP y que el
  // beneficiario sea una de las cuentas de esa sesión (se valida abajo, cuando
  // ya se resolvió el beneficiario por email o QR).
  let sesionCliente = null;
  if (!selfService) {
    const staff = await rewardsStaffFrom(req);
    if (rewardsStaffDenied(res, staff)) return;
    quienRegistra = staff;
  } else {
    sesionCliente = rewardsSesionDe(req);
    if (!sesionCliente) {
      return res.status(401).json({ ok: false, error: 'entra a tu cuenta para reclamar los puntos de una renta' });
    }
  }
  // La vista previa es herramienta del MOSTRADOR (con staff autenticado). En
  // self-service sería un oráculo silencioso: consultar titulares de órdenes
  // sin dejar rastro en el Ledger (review 31-jul-2026).
  if (body.preview && selfService) {
    return res.status(403).json({ ok: false, error: 'la vista previa es solo del mostrador' });
  }
  const orderNumber = parseInt(body.order_number, 10);
  if (!Number.isFinite(orderNumber)) return res.status(400).json({ ok: false, error: 'order_number invalido' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'atribuciones deshabilitadas (Ledger no configurado)' });

  try {
    // 1) el beneficiario: QR, correo o celular (v8.38). El customer_id explícito
    // solo se acepta en el mostrador — en self-service el candado de abajo
    // (la cuenta tiene que ser de su propia sesión) lo volvería a rechazar,
    // pero es más limpio no ofrecer siquiera esa puerta al cliente.
    const rb = await rewardsResolverMiembro(body, { mostrador: !selfService });
    if (rewardsResolverFallo(res, rb)) return;
    const cdb = await booqableGet('/customers/' + rb.customerId);
    const beneficiario = cdb.data;
    if (!beneficiario) return res.status(404).json({ ok: false, error: 'no se encontro esa cuenta' });
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(beneficiario.id)) {
      return res.status(403).json({ ok: false, error: 'esa cuenta no participa en Filmorent Rewards' });
    }
    // El cliente solo puede acreditarse puntos A SÍ MISMO: el beneficiario tiene
    // que ser una de las cuentas de su propia sesión (mismo patrón que
    // /rewards/member?customer_id=). El mostrador sí puede acreditar a terceros
    // porque ahí hay un humano identificado que responde por el movimiento.
    if (selfService && sesionCliente.ids.indexOf(beneficiario.id) === -1) {
      return res.status(403).json({ ok: false, error: 'solo puedes reclamar los puntos para tu propia cuenta' });
    }

    // 2) la orden: debe existir, no estar cancelada ni ser venta de equipo
    if (REWARDS_EXCLUDED_ORDER_NUMBERS.has(orderNumber)) {
      return res.status(409).json({ ok: false, error: 'las ventas de equipo no acumulan puntos' });
    }
    const od = await booqableGet('/orders?filter[number]=' + orderNumber + '&page[size]=2');
    const order = (od.data || [])[0];
    if (!order) return res.status(404).json({ ok: false, error: 'no existe la orden #' + orderNumber });
    const oa = order.attributes || {};
    if (oa.status === 'canceled' || oa.status === 'draft' || oa.status === 'concept') {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' no cuenta para puntos (' + oa.status + ')' });
    }
    if (String(oa.customer_id) === String(beneficiario.id)) {
      return res.status(409).json({ ok: false, error: 'esa orden ya es de esa cuenta: los puntos le llegan solos' });
    }
    // Self-service: solo rentas recientes (evita que alguien reclame el histórico ajeno)
    const dias = (Date.now() - new Date(oa.starts_at || oa.created_at).getTime()) / 86400000;
    if (selfService && dias > REWARDS_ATRIB_DIAS) {
      return res.status(409).json({ ok: false, error: 'esa renta tiene más de ' + REWARDS_ATRIB_DIAS + ' días; pídelo en el mostrador' });
    }

    // 3) puntos de esa orden (misma regla de siempre: base sin IVA, sin líneas excluidas)
    const gt = oa.grand_total_in_cents || 0;
    const gtt = oa.grand_total_with_tax_in_cents || 0;
    let excl = 0;
    try {
      const ld = await booqableGet('/lines?filter[order_id]=' + order.id + '&page[size]=100');
      const ratio = gtt ? (gt / gtt) : 1;
      for (const l of (ld.data || [])) {
        const la = l.attributes || {};
        if (la.archived || !rewardsLineExcluded(la.title)) continue;
        excl += Math.round((la.price_in_cents || 0) * ratio);
      }
    } catch (e2) { /* si falla, se atribuye el total */ }
    const baseCents = Math.max(0, gt - excl);
    // Los puntos viajan CON el multiplicador que la orden le ganó al titular
    // (auditoría 5-ago-2026): registrar 1x cuando el titular es Plata/Oro dejaba
    // la diferencia en su cuenta — la orden quedaba repartida entre dos cuentas,
    // justo lo que los términos §7 prohíben.
    let multOrden = 1;
    try {
      const earnedTit = await rewardsComputeEarned(oa.customer_id);
      const ordTit = (earnedTit.orders || []).find(o2 => String(o2.id) === String(order.id));
      if (ordTit && ordTit.mult) multOrden = ordTit.mult;
    } catch (e4) { /* sin dato del titular: 1x conservador */ }
    const puntos = Math.floor((baseCents * multOrden) / 100 / 100);
    if (puntos <= 0) return res.status(409).json({ ok: false, error: 'esa orden no genera puntos' });

    // 4) titular actual (para el registro y para restarle los puntos)
    let titularNombre = '';
    try {
      const td = await booqableGet('/customers/' + oa.customer_id);
      titularNombre = rewardsCleanName((td.data.attributes || {}).name);
    } catch (e3) { /* informativo */ }

    // 4.5) PREVIEW: el mostrador enseña los dos nombres juntos y pide confirmar
    // ANTES de escribir nada. En la prueba con el equipo (31-jul-2026) se atribuyó
    // una orden de práctica a un cliente real sin querer: la pantalla nunca puso
    // enfrente "la renta es de X → los puntos van a Y".
    if (body.preview) {
      let yaDe = null;
      try {
        const q = await fetch(REWARDS_SHEETS_URL + '?action=atribucion&order_number=' + orderNumber,
          { redirect: 'follow' }).then(r2 => r2.json());
        if (q && q.found) yaDe = (q.atribucion || {}).beneficiario || 'otra cuenta';
      } catch (e5) { /* si el Ledger no responde, el POST real lo vuelve a checar */ }
      return res.json({
        ok: true,
        preview: true,
        order_number: orderNumber,
        puntos: puntos,
        monto_mxn: baseCents / 100,
        beneficiario: rewardsCleanName((beneficiario.attributes || {}).name),
        beneficiario_email: (beneficiario.attributes || {}).email || '',
        titular: titularNombre,
        ya_atribuida_a: yaDe
      });
    }

    // 5) registrar en el Ledger (rechaza si la orden ya tiene dueño de puntos)
    const wrote = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tipo: 'atribucion',
        k: process.env.REWARDS_HITOS_KEY,   // candado global del Ledger (auditoría 19-ago)
        fecha: new Date().toISOString(),
        order_number: String(orderNumber),
        order_id: order.id,
        titular_id: oa.customer_id,
        titular_nombre: rewardsCellSafe(titularNombre),
        beneficiario_id: beneficiario.id,
        beneficiario_nombre: rewardsCellSafe(rewardsCleanName((beneficiario.attributes || {}).name)),
        beneficiario_email: (beneficiario.attributes || {}).email || '',
        monto_mxn: baseCents / 100,
        puntos: puntos,
        canal: body.canal || 'mostrador',
        registrado_por: rewardsCellSafe(quienRegistra),
        notas: rewardsCellSafe(String(body.notas || '')),
        ip: rewardsClientIp(req),
        ua: rewardsClientUa(req)
      }),
      redirect: 'follow'
    }).then(r => r.json()).catch(() => null);

    if (!wrote) return res.status(502).json({ ok: false, error: 'no se pudo registrar, intenta de nuevo' });
    if (wrote.ok === false) {
      return res.status(409).json({
        ok: false,
        error: wrote.error === 'orden ya atribuida'
          ? 'los puntos de la orden #' + orderNumber + ' ya son de ' + (wrote.beneficiario || 'otra cuenta')
          : (wrote.error || 'no se pudo registrar'),
        beneficiario: wrote.beneficiario, registrado_por: wrote.registrado_por, fecha: wrote.fecha
      });
    }

    // 6) espejo visible en Booqable: etiqueta en la orden (no cambia titular ni factura)
    let etiquetada = false;
    try {
      const tags = Array.isArray(oa.tag_list) ? oa.tag_list.slice() : [];
      const etiqueta = 'rw:' + rewardsQrCode(beneficiario.id);
      if (tags.indexOf(etiqueta) === -1) tags.push(etiqueta);
      await booqableWrite('PATCH', '/orders/' + order.id, {
        data: { id: order.id, type: 'orders', attributes: { tag_list: tags } }
      });
      etiquetada = true;
    } catch (e4) {
      console.error('[rewards] no se pudo etiquetar la orden ' + orderNumber + ': ' + e4.message);
    }

    console.log('[rewards] atribucion orden ' + orderNumber + ' (' + puntos + ' pts) -> ' +
      rewardsCleanName((beneficiario.attributes || {}).name) + ' | canal ' + (body.canal || 'mostrador') +
      ' | por ' + quienRegistra + (etiquetada ? '' : ' | SIN etiqueta en Booqable'));

    return res.json({
      ok: true,
      order_number: orderNumber,
      puntos: puntos,
      beneficiario: rewardsCleanName((beneficiario.attributes || {}).name),
      beneficiario_email: (beneficiario.attributes || {}).email || '',
      titular: titularNombre,
      etiquetada_en_booqable: etiquetada,
      registrado_por: quienRegistra
    });
  } catch (e) {
    console.error('[rewards] atribuir error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error registrando la atribucion, intenta de nuevo' });
  }
});

// ── GET /rewards/atribucion?n=1234 ──────────────────────────
// ¿esta orden ya tiene dueño de puntos? Lo consulta el mostrador antes de atribuir,
// para no discutir a ciegas cuando un cliente reclama.
app.get('/rewards/atribucion', async (req, res) => {
  const staff = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staff)) return;
  const n = parseInt(req.query.n, 10);
  if (!Number.isFinite(n)) return res.status(400).json({ ok: false, error: 'falta n (numero de orden)' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'atribuciones deshabilitadas' });
  try {
    const r = await fetch(REWARDS_SHEETS_URL + '?action=atribucion&order_number=' + n, { redirect: 'follow' });
    const j = await r.json().catch(() => null);
    if (!j) return res.status(502).json({ ok: false, error: 'no se pudo leer el Ledger' });
    return res.json(j);
  } catch (e) {
    return res.status(502).json({ ok: false, error: 'error consultando la atribucion' });
  }
});

// ── GET /rewards/folio?f=RWD-...&pin= ───────────────────────
// Para staff (F1.5): consulta el estado de un folio de canje en el Ledger
// (Apps Script doGet action=folio). No toca Booqable. El .gs responde
// {ok, found, folio:{folio,fecha,customer_id,email,nombre,reward,points,
//  discount_pct,estado,orden_aplicada}}.
app.get('/rewards/folio', async (req, res) => {
  const staffFolio = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffFolio)) return;
  const folio = String(req.query.f || '').trim().toUpperCase();
  if (folio.indexOf('RWD-') !== 0 || folio.length < 6) {
    return res.status(400).json({ ok: false, error: 'folio invalido (esperado RWD-...)' });
  }
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'consulta de folios deshabilitada (Ledger no configurado)' });
  try {
    const r = await fetch(REWARDS_SHEETS_URL + '?action=folio&folio=' + encodeURIComponent(folio), { redirect: 'follow' });
    if (!r.ok) return res.status(502).json({ ok: false, error: 'no se pudo leer el Ledger, intenta de nuevo' });
    const j = await r.json().catch(() => null);
    if (!j || j.ok === false) return res.status(502).json({ ok: false, error: 'no se pudo leer el Ledger, intenta de nuevo' });
    if (!j.found) return res.status(404).json({ ok: false, found: false, error: 'folio no encontrado' });
    console.log('[rewards] folio ' + folio + ' -> ' + ((j.folio || {}).estado || '?'));
    return res.json({ ok: true, found: true, folio: j.folio });
  } catch (e) {
    console.error('[rewards] folio error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error consultando el folio, intenta de nuevo' });
  }
});

// ── POST /rewards/folio/aplicar  {folio, order_number, staff_name?, pin?} ──
// Para staff (F1.5): marca un folio de canje como aplicado a una orden.
// POST al Apps Script {tipo:'aplicar', folio, order_number, staff_name};
// el .gs responde {ok, updated:bool, estado_previo}.
app.post('/rewards/folio/aplicar', async (req, res) => {
  const body = req.body || {};
  const staffAplicar = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffAplicar)) return;
  const folio = String(body.folio || '').trim().toUpperCase();
  const orderNumber = String(body.order_number || '').trim();
  if (folio.indexOf('RWD-') !== 0 || folio.length < 6) {
    return res.status(400).json({ ok: false, error: 'folio invalido (esperado RWD-...)' });
  }
  if (!orderNumber) return res.status(400).json({ ok: false, error: 'order_number requerido' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'aplicacion de folios deshabilitada (Ledger no configurado)' });
  try {
    // Términos §4 también en el camino del folio (auditoría 5-ago-2026): antes
    // este endpoint solo marcaba el Ledger y NINGUNA capa validaba la orden —
    // los candados vivían solo en /pagar. Mismos checks, solo lectura.
    let creditoCents = 0;
    try {
      const fq = await fetch(REWARDS_SHEETS_URL + '?action=folio&folio=' + encodeURIComponent(folio),
        { redirect: 'follow' }).then(r2 => r2.json());
      if (fq && fq.ok && !fq.found) return res.status(404).json({ ok: false, error: 'folio no encontrado' });
      if (fq && fq.found) {
        const est = String((fq.folio || {}).estado || 'pendiente');
        if (est !== 'pendiente') {
          return res.status(409).json({
            ok: false, updated: false, estado_previo: est,
            error: est === 'aplicado' ? 'el folio ya estaba aplicado' : ('el folio no se puede aplicar (estado: ' + est + ')')
          });
        }
        // El crédito viene en el nombre de la recompensa ("Crédito de $700 en ...")
        const m = String((fq.folio || {}).reward || '').match(/\$\s?([\d,]+)/);
        if (m) creditoCents = parseInt(m[1].replace(/,/g, ''), 10) * 100;
      }
    } catch (eF) { /* si el Ledger no responde aquí, el POST de abajo lo re-checa */ }

    const odF = await booqableGet('/orders?filter[number]=' + orderNumber + '&page[size]=2');
    const orderF = (odF.data || [])[0];
    if (!orderF) return res.status(404).json({ ok: false, error: 'no existe la orden #' + orderNumber });
    const oaF = orderF.attributes || {};
    if (oaF.status === 'canceled') {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' esta cancelada' });
    }
    if (oaF.status === 'started' || oaF.status === 'stopped') {
      return res.status(409).json({
        ok: false,
        error: 'la orden #' + orderNumber + (oaF.status === 'started' ? ' ya esta en curso' : ' ya termino') +
          ': el descuento aplica solo en rentas nuevas, antes de facturar'
      });
    }
    if (oaF.payment_status === 'paid') {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' ya esta pagada: el descuento se aplica antes del pago' });
    }
    const ldF = await booqableGet('/lines?filter[order_id]=' + orderF.id + '&page[size]=100');
    const lineasF = (ldF.data || []).filter(l => !((l.attributes || {}).archived));
    const esVentaF = lineasF.some(l => {
      const t = String((l.attributes || {}).title || '').toLowerCase().trim();
      return t.indexOf('venta ') === 0 || t.indexOf('venta-') === 0 || t.indexOf('venta:') === 0;
    });
    if (esVentaF) {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' incluye venta de equipo: los descuentos Rewards aplican solo en rentas' });
    }
    // Otro crédito Rewards en la orden (de /pagar u otro folio) = doble descuento.
    // Si la línea menciona ESTE folio es el descuento manual de este mismo canje.
    const rewardsAjena = lineasF.some(l => {
      const t = String((l.attributes || {}).title || '');
      return t.toLowerCase().indexOf('filmorent rewards') === 0 && t.toUpperCase().indexOf(folio) === -1;
    });
    if (rewardsAjena) {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' ya tiene otro descuento Rewards aplicado' });
    }
    if (creditoCents > 0) {
      // Si el descuento de este folio ya está en la orden, el total ya bajó:
      // exigir 2x sobre ese total castigaría el flujo legítimo — el piso baja a 1x.
      const descuentoYaEnOrden = lineasF.some(l =>
        String((l.attributes || {}).title || '').toUpperCase().indexOf(folio) >= 0);
      const minimo = descuentoYaEnOrden ? creditoCents : creditoCents * 2;
      const twt = oaF.grand_total_with_tax_in_cents || 0;
      if (twt < minimo) {
        return res.status(409).json({
          ok: false,
          error: 'la orden debe ser de al menos ' + rewardsFormatMXN(creditoCents * 2) +
            ' (2x el descuento de ' + rewardsFormatMXN(creditoCents) + '); total actual con IVA: ' + rewardsFormatMXN(twt)
        });
      }
    }

    const r = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tipo: 'aplicar',
        k: process.env.REWARDS_HITOS_KEY,   // candado global del Ledger (auditoría 19-ago)
        folio: folio,
        order_number: orderNumber,
        staff_name: staffAplicar   // identidad verificada, no el body
      }),
      redirect: 'follow'
    });
    if (!r.ok) return res.status(502).json({ ok: false, error: 'no se pudo escribir al Ledger, intenta de nuevo' });
    const j = await r.json().catch(() => null);
    if (!j) return res.status(502).json({ ok: false, error: 'respuesta invalida del Ledger, intenta de nuevo' });
    if (j.ok === false) {
      return res.status(404).json({ ok: false, error: j.error || 'folio no encontrado' });
    }
    if (!j.updated) {
      const prev = j.estado_previo || 'desconocido';
      return res.status(409).json({
        ok: false,
        updated: false,
        estado_previo: prev,
        error: prev === 'aplicado' ? 'el folio ya estaba aplicado' : ('el folio no se pudo aplicar (estado: ' + prev + ')')
      });
    }
    console.log('[rewards] folio ' + folio + ' aplicado a orden ' + orderNumber +
      ' por ' + staffAplicar);
    return res.json({ ok: true, updated: true, estado_previo: j.estado_previo || 'pendiente' });
  } catch (e) {
    console.error('[rewards] folio/aplicar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error aplicando el folio, intenta de nuevo' });
  }
});


// =====================================================================
// v8.9: BORRADOR DE ORDEN desde la conversacion (copiloto del equipo)
// POST /webhook/draft-order  body: { contactId }
// Disparado por un Shortcut en Respond.io. Lee la conversacion, extrae
// con Claude el equipo y las fechas, crea una orden BORRADOR en Booqable
// (status draft — nada le llega al cliente) y deja un comentario interno
// en la conversacion con el numero de orden y lo que falto por matchear.
// Seguridad: si DRAFT_ORDER_TOKEN esta seteada, exige header x-draft-token.
// =====================================================================

const DRAFT_ORDER_TOKEN = process.env.DRAFT_ORDER_TOKEN || '';

// Memoria corta contactId -> {at, numbers} para avisar de posibles duplicados
// cuando alguien aprieta el boton dos veces sobre la misma conversacion.
const draftRecientes = {};

async function draftRespondioGet(path) {
  const r = await fetch('https://api.respond.io/v2' + path, {
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' }
  });
  if (!r.ok) throw new Error('Respond.io ' + r.status + ' en ' + path);
  return r.json();
}

async function draftPostComment(contactId, text) {
  try {
    const r = await fetch('https://api.respond.io/v2/contact/id:' + contactId + '/comment', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: String(text).slice(0, 1950) }) // la API corta en 2000
    });
    if (!r.ok) console.error('[draft-order] comentario fallo: ' + r.status + ' - ' + (await r.text()));
    return r.ok;
  } catch (e) {
    console.error('[draft-order] comentario error: ' + e.message);
    return false;
  }
}

// Generado del censo de 31,690 ordenes de Booqable (3-ago-2026).
// Que equipo se pone de verdad cuando el cliente pide algo generico.
// Solo entradas con >=40% de las ordenes; por debajo se prefiere preguntar.
const PREFERENCIAS = {
  'tripie': {
    foto:        { nombre: "Tripié Sachtler ACE M MS", conf: 49 },
  },
  'estabilizador': {
    cine_grande: { nombre: "Estabilizador DJI RONIN RS4 Pro", conf: 83 },
    cine_chica:  { nombre: "Estabilizador DJI RONIN RS4 Pro", conf: 51 },
  },
  'sandbag': {
    global:      { nombre: "Sandbag - Rojo", conf: 45 },
    foto:        { nombre: "Sandbag - Rojo", conf: 44 },
  },
  'extension': {
    global:      { nombre: "Extensión (diferente medida) - 10 mts", conf: 46 },
    cine_grande: { nombre: "Extensión (diferente medida) - 10 mts", conf: 41 },
    cine_chica:  { nombre: "Extensión (diferente medida) - 10 mts", conf: 45 },
    foto:        { nombre: "Extensión (diferente medida) - 10 mts", conf: 47 },
  },
  'cstand': {
    global:      { nombre: "C-Stand (Century) - Negro, No Desmontable, Sin ruedas", conf: 40 },
    foto:        { nombre: "C-Stand (Century) - Negro, No Desmontable, Sin ruedas", conf: 40 },
  },
};

// Detecta la familia de camara del pedido para elegir el accesorio adecuado.
function draftFamilia(textos) {
  const t = (textos || []).join(' ').toLowerCase();
  if (/fx6|fx9|\bred\b|scarlet|venice/.test(t)) return 'cine_grande';
  if (/fx3|fx30|fx5/.test(t)) return 'cine_chica';
  if (/a7|a6400|a6700|r5|r6|z6|zv-e10/.test(t)) return 'foto';
  return null;
}

// Busca un product group por texto y regresa {groupName, productId} o null.
// PROBLEMA CONOCIDO de filter[q] con varias palabras: regresa TODO lo que
// matchee CUALQUIER palabra, en orden alfabetico — para "sony fx3" la camara
// FX3 ni aparece en la primera pagina (salen adaptadores y baterias). Por eso
// juntamos candidatos de VARIAS busquedas (incluida cada palabra-modelo tipo
// "fx3"/"200x" a solas, que si la regresa) y luego exigimos que el nombre
// contenga las palabras pedidas como PALABRA COMPLETA ("grand" != "Grande",
// "fx3" != "FX30"). Sin match confiable regresa null: mejor "agregar a mano"
// que adivinar un producto equivocado.
async function draftFindProduct(query, contexto) {
  query = String(query || '').replace(/\([^)]*\)/g, ' ').replace(/["']/g, ' ');
  const norm = function (s) {
    return String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  };
  const contienePalabra = function (texto, w) {
    const esc = w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    if (new RegExp('(^|[^a-z0-9])' + esc + '($|[^a-z0-9])').test(texto)) return true;
    // Numero PURO: acepta que el catalogo le pegue una letra de variante
    // ("amaran 300" -> "Amaran 300C RGB"). Solo para numeros solos: "fx3"
    // NO debe matchear "FX30" porque son camaras distintas.
    if (/^\d+$/.test(w) && new RegExp('(^|[^a-z0-9])' + esc + '[a-z]($|[^a-z0-9])').test(texto)) return true;
    return w.length >= 6 && texto.replace(/\s+/g, '').indexOf(w) !== -1;
  };
  const genericas = ['luz', 'luces', 'lampara', 'camara', 'lente', 'equipo', 'kit', 'de', 'para', 'con',
    'el', 'la', 'un', 'una', 'unos', 'unas', 'dos', 'tres', 'cuatro', 'cinco', 'par'];
  const words = norm(query).split(/\s+/).filter(function (w) { return w.length > 1; });
  if (!words.length) return null;
  const significativas = words.filter(function (w) { return genericas.indexOf(w) === -1; });
  const modelos = significativas.filter(function (w) { return /\d/.test(w); });

  // Pool de candidatos: varias busquedas, deduplicadas por id.
  const queries = [words.join(' ')];
  if (significativas.length && significativas.length < words.length) queries.push(significativas.join(' '));
  modelos.forEach(function (w) { queries.push(w); });
  if (significativas.length > 2) queries.push(significativas.slice(0, 2).join(' '));
  const vistos = {};
  const candidatos = [];
  // Se buscan BUNDLES y productos. Los bundles ("Kit Camara Sony A6400 con
  // baterias, cargador y SD") son lo que el equipo agenda de verdad: reservar el
  // producto suelto deja fuera baterias, cargador y memoria (queja de Barush).
  for (const q of queries) {
    if (!q || vistos['q:' + q]) continue;
    vistos['q:' + q] = 1;
    for (const recurso of ['bundles', 'product_groups']) {
      let d;
      try {
        d = await booqableGet('/' + recurso + '?filter[q]=' + encodeURIComponent(q) + '&page[size]=50');
      } catch (e) { continue; }
      (d.data || []).forEach(function (x) {
        if (!x.attributes || x.attributes.archived || vistos[x.id]) return;
        vistos[x.id] = 1;
        x.esBundle = (recurso === 'bundles');
        candidatos.push(x);
      });
    }
  }
  if (!candidatos.length) return null;

  // Exigencia por niveles: todas las palabras -> las significativas ->
  // las primeras 2 significativas -> solo el modelo (fx3, 200x).
  // El cliente separa lo que el catalogo junta: "Sony A7 IV" vs "Camara Sony A7IV".
  // Se pegan los tokens cortos contiguos cuando alguno trae digito (a7+iv -> a7iv),
  // asi "a7iv" si matchea como palabra completa (y sigue sin matchear "A7III").
  const pegarModelo = function (ws) {
    const out = [];
    for (let i = 0; i < ws.length; i++) {
      const a = ws[i], b = ws[i + 1];
      // Solo modelo+sufijo: el token traer digito ("a7", "fx3") y el siguiente
      // ser un sufijo corto SIN digito ("iv", "iii", "s"). Asi no pega "sony"+"a7".
      if (b && /\d/.test(a) && !/\d/.test(b) && b.length <= 3) {
        out.push(a + b); i++;
      } else out.push(a);
    }
    return out;
  };
  const wordsPegados = pegarModelo(words);
  const significativasPegadas = pegarModelo(significativas);
  const niveles = [words];
  if (wordsPegados.join(' ') !== words.join(' ')) niveles.push(wordsPegados);
  if (significativasPegadas.join(' ') !== significativas.join(' ')) niveles.push(significativasPegadas);
  if (significativas.length && significativas.join(' ') !== words.join(' ')) niveles.push(significativas);
  if (significativas.length > 2) niveles.push(significativas.slice(0, 2));
  // OJO: NO se agrega un nivel con solo el numero. "amaran 300" caia a ["300"]
  // y traia "Lampara de tungsteno Arri 300". Mejor no encontrar que inventar.
  // Pedido GENERICO = ninguna palabra trae numero de modelo ("un tripie", "una luz").
  // Ahi el catalogo tiene 8+ opciones validas y elegir sola es como cayo un
  // "Tripie People" (fuera de tienda, medio danado, solo para el prompter) en una
  // orden real. En ese caso NO se elige: se le pregunta al equipo.
  const esGenerico = !significativas.some(function (w) { return /\d/.test(w); });
  for (const setPalabras of niveles) {
    if (!setPalabras.length) continue;
    let exactos = candidatos.filter(function (x) {
      const n = norm(x.attributes.name);
      return setPalabras.every(function (w) { return contienePalabra(n, w); });
    });
    if (!exactos.length) continue;
    // Lo que no se publica en la tienda no se ofrece solo (equipo interno,
    // danado o de uso especifico). Solo se usa si NO hay nada publicado.
    const enTienda = exactos.filter(function (x) { return x.attributes.show_in_store !== false; });
    if (enTienda.length) exactos = enTienda;
    if (esGenerico && exactos.length > 1) {
      // Antes de rendirse: ¿que pone el equipo de verdad en estos casos?
      // (censo de 31,690 ordenes de Booqable). Si el historial es claro se usa
      // ese y se DICE el porcentaje; si esta repartido, se pregunta.
      const cat = Object.keys(PREFERENCIAS).find(function (c) {
        return significativas.some(function (w) { return w.indexOf(c.slice(0, 6)) === 0 || c.indexOf(w) === 0; });
      });
      const fam = draftFamilia(contexto);
      const pref = cat ? (PREFERENCIAS[cat][fam] || PREFERENCIAS[cat].global) : null;
      if (pref) {
        const elegido = exactos.find(function (x) {
          return norm(x.attributes.name).indexOf(norm(pref.nombre).slice(0, 22)) !== -1;
        });
        if (elegido) {
          const salida = { groupName: elegido.attributes.name.trim(), porHistorial: pref.conf };
          if (elegido.esBundle) { salida.bundleId = elegido.id; salida.esBundle = true; return salida; }
          try {
            const pd = await booqableGet('/products?filter[product_group_id]=' + elegido.id + '&page[size]=1');
            const p = (pd.data || [])[0];
            if (p) { salida.productId = p.id; return salida; }
          } catch (e) { /* cae a preguntar */ }
        }
      }
      return {
        ambiguo: true,
        opciones: exactos.slice(0, 6).map(function (x) { return x.attributes.name.trim(); })
      };
    }
    // Con todo lo demas igual, el KIT le gana al producto suelto.
    exactos.sort(function (a, b) {
      if (!!b.esBundle !== !!a.esBundle) return b.esBundle ? 1 : -1;
      return a.attributes.name.length - b.attributes.name.length;
    });
    const g = exactos[0];
    if (g.esBundle) return { groupName: g.attributes.name.trim(), bundleId: g.id, esBundle: true };
    try {
      const pd = await booqableGet('/products?filter[product_group_id]=' + g.id + '&page[size]=1');
      const p = (pd.data || [])[0];
      if (p) return { groupName: g.attributes.name.trim(), productId: p.id };
    } catch (e) { /* siguiente nivel */ }
  }
  // Nada matcheo por texto. Los clientes piden el equipo como ELLOS lo conocen
  // ("la amaran grande", "un cangrejo", "la camarita chica"), no como se llama en
  // Booqable, asi que aqui se le pregunta al modelo cual de los candidatos es —
  // pero solo eligiendo de la lista real del catalogo, nunca inventando.
  if (candidatos.length && anthropic) {
    const lista = candidatos.slice(0, 40);
    try {
      const resp = await anthropic.messages.create({
        model: 'claude-opus-5',
        max_tokens: 300,
        thinking: { type: 'disabled' },
        messages: [{
          role: 'user',
          content: 'Filmorent renta equipo audiovisual en Monterrey. Un cliente pidio por WhatsApp: "' +
            query + '".\n\nEstos son los productos del catalogo que podrian corresponder:\n' +
            lista.map(function (c, i) { return (i + 1) + '. ' + c.attributes.name; }).join('\n') +
            '\n\nResponde SOLO con el numero del que corresponde. Si ninguno corresponde con ' +
            'CERTEZA, responde 0. Ante la duda responde 0: es mejor que un humano lo agregue a ' +
            'mano que poner el equipo equivocado en una orden.'
        }]
      });
      const n = parseInt((claudeText(resp).match(/\d+/) || ['0'])[0], 10);
      if (n >= 1 && n <= lista.length) {
        const g = lista[n - 1];
        console.log('[draft-order] "' + query + '" -> "' + g.attributes.name + '" (elegido por IA)');
        if (g.esBundle) return { groupName: g.attributes.name.trim(), bundleId: g.id, esBundle: true, porIA: true };
        const pd = await booqableGet('/products?filter[product_group_id]=' + g.id + '&page[size]=1');
        const p = (pd.data || [])[0];
        if (p) return { groupName: g.attributes.name.trim(), productId: p.id, porIA: true };
      }
    } catch (e) { console.error('[draft-order] desempate IA: ' + e.message); }
  }
  return null;
}

app.post('/webhook/draft-order', async (req, res) => {
  try {
    if (DRAFT_ORDER_TOKEN && (req.headers['x-draft-token'] || '') !== DRAFT_ORDER_TOKEN) {
      return res.status(401).json({ ok: false, error: 'token invalido' });
    }
    if (!BOOQABLE_API_KEY) {
      return res.status(500).json({ ok: false, error: 'BOOQABLE_API_KEY no configurada' });
    }
    const contactId = extractContactId(req.body);
    if (!contactId || !/^\d+$/.test(String(contactId))) {
      return res.status(400).json({ ok: false, error: 'contactId faltante o invalido' });
    }

    console.log('[draft-order] solicitud para contacto ' + contactId);

    // 1) Contacto + mensajes recientes
    const [contact, messagesData] = await Promise.all([
      draftRespondioGet('/contact/id:' + contactId),
      draftRespondioGet('/contact/id:' + contactId + '/message/list?limit=60')
    ]);
    const msgs = (messagesData.items || messagesData.data || []).slice().reverse(); // cronologico
    // Marcar explicitamente cuando Filmorent ya mando una cotizacion/orden en PDF:
    // todo lo pedido ANTES de eso ya se atendio y no debe volver a convertirse en
    // borrador (asi es como el copiloto revivia rentas viejas ya cotizadas).
    let idxUltimoDoc = -1;
    // Imagenes que mando el CLIENTE (listas de equipo en foto/captura — caso #10801):
    // se bajan y se pasan a Claude con vision. Antes solo salia "[image]" en el
    // transcript y el borrador quedaba vacio.
    const imagenesCliente = [];
    const filas = msgs.map(function (m, i) {
      const who = m.traffic === 'incoming' ? 'CLIENTE' : 'FILMORENT';
      const msg = m.message || {};
      // Los correos NO usan message.text sino message.message, y traen subject.
      // Sin esto el transcript de un canal de email salia vacio.
      let t = msg.text || msg.message;
      if (msg.type === 'email' && t) {
        // Cortar la cadena citada y la firma: solo estorban.
        t = String(t)
          .split(/\n\s*(?:On .{0,80}wrote:|El .{0,80}escribi|On \d{1,2} \w+ \d{4},|-{2,}\s*Original)/)[0]
          .split(/\n\s*(?:Best,|Saludos,|Atentamente,)\s*\n/)[0];
        if (msg.subject) t = '(asunto: ' + msg.subject + ') ' + t;
      }
      if (!t && msg.type === 'attachment') {
        const att = msg.attachment || {};
        const nombre = String(att.fileName || '').toLowerCase();
        const urlAtt = att.url || att.fileUrl || '';
        const esDoc = att.type === 'file' || /invoice|cotiza|orden|pro_forma|proforma|\.pdf/.test(nombre);
        const esImagen = att.type === 'image' || /\.(jpe?g|png|webp|gif)(\?|$)/.test(String(urlAtt).toLowerCase());
        if (who === 'FILMORENT' && esDoc) {
          idxUltimoDoc = i;
          t = '[FILMORENT LE ENVIO UN DOCUMENTO (cotizacion/orden): ' + (att.fileName || 'documento') + ']';
        } else if (who === 'CLIENTE' && esImagen && urlAtt) {
          imagenesCliente.push({ idx: i, url: urlAtt });
          t = '[IMAGEN ' + imagenesCliente.length + ' DEL CLIENTE — va adjunta abajo, LEELA]';
        } else {
          t = '[' + (att.type || 'adjunto') + ']';
        }
      }
      if (!t) t = '[' + (msg.type || 'adjunto') + ']';
      return who + ': ' + String(t).replace(/\s+/g, ' ').slice(0, 300);
    });
    if (idxUltimoDoc >= 0 && idxUltimoDoc < filas.length - 1) {
      filas.splice(idxUltimoDoc + 1, 0,
        '--- TODO LO DE ARRIBA YA SE ATENDIO (Filmorent ya envio el documento). ' +
        'SOLO lo de aqui para abajo puede estar pendiente. ---');
    }
    const transcript = filas.join('\n').slice(-8000);

    if (!transcript) {
      return res.status(422).json({ ok: false, error: 'conversacion vacia' });
    }

    // Bajar las imagenes del cliente (solo las posteriores al ultimo documento enviado —
    // las de antes ya se atendieron) y prepararlas como bloques de vision para Claude.
    const imagenesRelevantes = (idxUltimoDoc >= 0
      ? imagenesCliente.filter(function (im) { return im.idx > idxUltimoDoc; })
      : imagenesCliente).slice(-8); // tope 8, las mas recientes
    const bloquesImagen = [];
    for (const im of imagenesRelevantes) {
      try {
        const ir = await fetch(im.url);
        if (!ir.ok) { console.error('[draft-order] imagen HTTP ' + ir.status); continue; }
        const ct = String(ir.headers.get('content-type') || '').split(';')[0].trim();
        if (['image/jpeg', 'image/png', 'image/webp', 'image/gif'].indexOf(ct) === -1) continue;
        const buf = Buffer.from(await ir.arrayBuffer());
        if (buf.length > 4500000) { console.error('[draft-order] imagen muy grande, la salto'); continue; }
        bloquesImagen.push({ type: 'image', source: { type: 'base64', media_type: ct, data: buf.toString('base64') } });
      } catch (e) { console.error('[draft-order] imagen no bajo: ' + e.message); }
    }
    if (imagenesRelevantes.length) {
      console.log('[draft-order] imagenes del cliente: ' + imagenesRelevantes.length + ' relevantes, ' + bloquesImagen.length + ' bajadas OK');
    }

    const nombreContacto = (((contact.firstName || '') + ' ' + (contact.lastName || '')).trim()) || null;
    const phone = String(contact.phone || '').replace(/[^0-9]/g, '');

    // 2) Claude extrae equipo y fechas
    // 4) Cliente en Booqable.
    // Un mismo humano suele tener VARIOS registros (el suyo y el de su empresa;
    // ej. tel 818254xxxx -> "Pasumecha Producciones" 79 ordenes y "Daniel Alonso"
    // 59 ordenes). Se juntan candidatos por TELEFONO y por NOMBRE, con su
    // historial, y solo se asigna solo cuando hay UNO. Si hay varios, se listan
    // con historial y se genera la pregunta para el cliente.
    const soloDigitos = function (s) { return String(s || '').replace(/[^0-9]/g, ''); };
    const normTxt = function (s) {
      return String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    };
    // Con UNA palabra compartida entraba mucho ruido ("Daniel Alonso" matcheaba
    // "Alfredo Lamas Daniel" y "Joseph Daniel Shay"). Si el contacto tiene nombre
    // y apellido, se exigen AL MENOS DOS palabras en comun.
    const compartePalabra = function (a, b) {
      const nb = normTxt(b);
      const ws = normTxt(a).split(/\s+/).filter(function (w) { return w.length > 2; });
      if (!ws.length) return false;
      const hits = ws.filter(function (w) { return nb.indexOf(w) !== -1; }).length;
      return ws.length >= 2 ? hits >= 2 : hits >= 1;
    };
    const resumeCliente = function (c) {
      const a = c.attributes;
      return {
        id: c.id,
        name: a.name,
        ordenes: a.order_count || 0,
        ultima: (a.latest_order_at || '').slice(0, 10),
        porTelefono: false
      };
    };
    const candidatos = [];
    const yaVisto = {};
    const phone10 = phone.slice(-10);
    if (phone10.length === 10) {
      try {
        const cd = await booqableGet('/customers?filter[q]=' + phone10 + '&page[size]=5');
        (cd.data || []).forEach(function (c) {
          const p = (c.attributes && c.attributes.properties) || {};
          const cuadra = [p.phone, p.phone_2].some(function (t) {
            const d = soloDigitos(t);
            return d && d.endsWith(phone10);
          });
          if (cuadra && !yaVisto[c.id]) {
            yaVisto[c.id] = 1;
            const r = resumeCliente(c);
            r.porTelefono = true;
            candidatos.push(r);
          }
        });
      } catch (e) { /* sin candidatos por telefono */ }
    }
    const emailContacto = String(contact.email || '').trim().toLowerCase();
    if (emailContacto) {
      try {
        const cd = await booqableGet('/customers?filter[q]=' + encodeURIComponent(emailContacto) + '&page[size]=5');
        (cd.data || []).forEach(function (c) {
          const a = c.attributes || {};
          if (yaVisto[c.id]) return;
          if (String(a.email || '').trim().toLowerCase() !== emailContacto) return;
          yaVisto[c.id] = 1;
          const r = resumeCliente(c);
          r.porTelefono = true; // match fuerte: sirve para asignar solo
          candidatos.push(r);
        });
      } catch (e) { /* sin candidatos por email */ }
    }
    if (nombreContacto) {
      try {
        const cd = await booqableGet('/customers?filter[q]=' + encodeURIComponent(nombreContacto) + '&page[size]=5');
        (cd.data || []).forEach(function (c) {
          if (yaVisto[c.id] || !c.attributes) return;
          // filter[q] por nombre es difuso: exigir que comparta palabra Y que
          // tenga historial real, para no sugerir homonimos al azar.
          if (!compartePalabra(nombreContacto, c.attributes.name)) return;
          if (!(c.attributes.order_count > 0)) return;
          yaVisto[c.id] = 1;
          candidatos.push(resumeCliente(c));
        });
      } catch (e) { /* sin candidatos por nombre */ }
    }
    candidatos.sort(function (a, b) { return (b.ultima || '').localeCompare(a.ultima || ''); });

    let customerId = null;
    let customerName = null;
    let clienteAprendido = false;
    if (candidatos.length === 1 && candidatos[0].porTelefono &&
        (!nombreContacto || compartePalabra(nombreContacto, candidatos[0].name))) {
      customerId = candidatos[0].id;
      customerName = candidatos[0].name;
    }
    // APRENDER DE LA DECISION DEL HUMANO: si en una orden anterior de ESTA
    // conversacion ya alguien asigno cliente, se usa el mismo. Asi el equipo
    // contesta "¿a nombre de quien?" UNA vez y no en cada borrador.
    if (!customerId) {
      try {
        const prev = await booqableGet('/orders?filter[tag_list]=' +
          encodeURIComponent('wa-' + contactId) + '&sort=-number&page[size]=10');
        const conCliente = (prev.data || []).find(function (o) {
          return o.attributes && o.attributes.customer_id && o.attributes.status !== 'canceled';
        });
        if (conCliente) {
          customerId = conCliente.attributes.customer_id;
          const cd = await booqableGet('/customers/' + customerId);
          customerName = cd.data.attributes.name;
          clienteAprendido = true;
          console.log('[draft-order] cliente aprendido de la orden #' +
            conCliente.attributes.number + ': ' + customerName);
        }
      } catch (e) { /* sigue sin cliente */ }
    }


    // Que ya tiene pedido este cliente en BOOQABLE (la fuente de verdad, no el
    // chat): sin esto el copiloto volvia a crear ordenes de cosas que el equipo
    // ya habia levantado. Se consultan las ordenes vigentes de los candidatos.
    const hoyIso = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Monterrey' });
    const desdeIso = new Date(Date.now() - 3 * 24 * 3600 * 1000).toISOString().slice(0, 10);
    // Fuentes: (a) las ordenes de los clientes candidatos y (b) las ordenes
    // etiquetadas con ESTA conversacion. La (b) es indispensable: cuando el
    // cliente queda sin asignar (persona vs empresa), la orden no aparece por
    // customer_id y el copiloto la volvia a crear.
    const tagConversacion = 'wa-' + contactId;
    const ordenesRelevantes = {};
    const consultas = candidatos.slice(0, 2).map(function (c) {
      return { url: '/orders?filter[customer_id]=' + c.id + '&sort=-number&page[size]=8', quien: c.name };
    });
    consultas.push({
      url: '/orders?filter[tag_list]=' + encodeURIComponent(tagConversacion) + '&sort=-number&page[size]=8',
      quien: 'esta conversacion'
    });
    for (const q of consultas) {
      let od;
      try { od = await booqableGet(q.url); } catch (e) { continue; }
      for (const o of (od.data || [])) {
        const a = o.attributes;
        if (!a || a.status === 'canceled') continue;
        if ((a.stops_at || '').slice(0, 10) < desdeIso) continue;
        if (!ordenesRelevantes[o.id]) ordenesRelevantes[o.id] = { o: o, quien: q.quien };
      }
    }
    const yaPedido = [];
    const ordenesVigentes = [];
    for (const k of Object.keys(ordenesRelevantes)) {
      const o = ordenesRelevantes[k].o;
      const a = o.attributes;
      let items = [];
      try {
        const ld = await booqableGet('/lines?filter[owner_id]=' + o.id + '&page[size]=25');
        items = (ld.data || [])
          .filter(function (l) { return l.attributes && l.attributes.item_id; })
          .map(function (l) { return (l.attributes.title || '').trim(); })
          .filter(Boolean);
      } catch (e) { /* sin detalle */ }
      yaPedido.push('#' + a.number + ' (' + a.status + ') ' + (a.starts_at || '').slice(0, 10) +
        ' a ' + (a.stops_at || '').slice(0, 10) + ': ' +
        (items.length ? items.slice(0, 12).join(', ') : 'sin equipo capturado'));
      ordenesVigentes.push({
        number: a.number, status: a.status,
        fi: (a.starts_at || '').slice(0, 10), ff: (a.stops_at || '').slice(0, 10),
        items: items
      });
    }
    const bloqueYaPedido = yaPedido.length
      ? '\nORDENES QUE ESTE CLIENTE YA TIENE EN EL SISTEMA (NO las vuelvas a crear):\n' +
        yaPedido.slice(0, 8).join('\n') + '\n'
      : '';

    const hoyMty = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Monterrey' });
    const extractPrompt = 'Eres el asistente interno de Filmorent (renta de equipo audiovisual en Monterrey). ' +
      'Lee esta conversacion de WhatsApp y arma los BORRADORES de orden que hagan falta.\n\n' +
      'HOY es ' + hoyMty + ' (zona America/Monterrey).\n\n' +
      'CONVERSACION (CLIENTE = el cliente, FILMORENT = nuestro equipo o el bot):\n' + transcript + '\n' +
      bloqueYaPedido + '\n' +
      'Regresa UNICAMENTE un objeto JSON valido, sin markdown ni texto extra:\n' +
      '{"solicitudes":[{' +
      '"equipos":[{"descripcion":"nombre CORTO, MAXIMO 5 palabras, SIN parentesis (los detalles van en notas), ej: Sony FX3, DJI Mini 4 Pro, luz Amaran 200x. Para estudios usa exactamente: Estudio Filmo Grand, Estudio Filmo Pocket o Estudio Podcast","cantidad":1}],' +
      '"fecha_inicio":"YYYY-MM-DD dia que EMPIEZA a usar, o null",' +
      '"fecha_regreso":"YYYY-MM-DD dia que REGRESA (si solo dijo hasta cuando lo USA, el regreso es la manana del dia siguiente), o null",' +
      '"hora_inicio":"HH:MM SOLO si dijo a que hora recoge, si no null",' +
      '"hora_regreso":"HH:MM SOLO si dijo a que hora regresa, si no null"' +
      '}],' +
      '"notas":"detalles utiles para el equipo (proyecto, dudas abiertas, entrega); breve",' +
      '"confianza":"alta|media|baja"}\n\n' +
      'REGLA MAS IMPORTANTE — UNA SOLICITUD POR RANGO DE FECHAS:\n' +
      'Si el cliente pidio cosas para DIAS DISTINTOS, son solicitudes SEPARADAS, NO las juntes.\n' +
      'Ejemplo: "dos FX3 para manana" + "una FX6 para el miercoles" = DOS solicitudes.\n' +
      'Un estudio por bloques ("3 dias de 4 horas cada dia") = UNA solicitud POR DIA, cada una con su horario.\n' +
      'Solo van juntos en la MISMA solicitud los equipos que se recogen y regresan en las MISMAS fechas.\n\n' +
      'QUE INCLUIR: SOLO lo que NO tiene orden todavia. Si arriba aparece "ORDENES QUE ESTE CLIENTE ' +
      'YA TIENE", eso YA ESTA LEVANTADO: NO lo repitas aunque se hable de ello en el chat ' +
      '(compara equipo Y fechas). Si en la conversacion aparece la linea ' +
      '"TODO LO DE ARRIBA YA SE ATENDIO", ignora por completo lo que este ARRIBA de esa linea: ' +
      'ya se cotizo y ya se le mando. Tampoco incluyas lo que solo pregunto por precio y descarto. ' +
      'Ante la duda de si algo ya se atendio, DEJALO FUERA: es mas facil que el equipo agregue una ' +
      'linea que darse cuenta de que sobra una orden.\n' +
      'Fechas relativas ("este viernes", "el fin") se calculan con la fecha de HOY. cantidad default 1. ' +
      'Si no hay nada pendiente que rentar, solicitudes = [].';

    // Extraccion con reintento: si la respuesta viene vacia o el JSON no parsea,
    // se intenta una segunda vez con instruccion mas dura. Cualquier fallo se le
    // AVISA al equipo en la conversacion — nunca fallar en silencio (el peor bug:
    // el empleado aprieta el boton y no pasa nada).
    const parseExt = function (raw) {
      if (!raw) return null;
      const ini = raw.indexOf('{');
      const fin = raw.lastIndexOf('}');
      if (ini === -1 || fin <= ini) return null;
      try { return JSON.parse(raw.slice(ini, fin + 1)); } catch (e) { return null; }
    };
    let ext = null;
    let ultimoRaw = '';
    for (let intento = 1; intento <= 2 && !ext; intento++) {
      let contenido = intento === 1
        ? extractPrompt
        : extractPrompt + '\n\nIMPORTANTE: tu respuesta anterior no fue JSON valido. ' +
          'Responde SOLO el objeto JSON, empezando con { y terminando con }. Sin texto antes ni despues.';
      // Con imagenes: van como bloques de vision + instruccion de leerlas.
      let userContent = contenido;
      if (bloquesImagen.length) {
        contenido += '\n\nADJUNTAS VAN ' + bloquesImagen.length + ' IMAGEN(ES) QUE MANDO EL CLIENTE ' +
          '(listas de equipo, capturas de la tienda, fotos). LEELAS CON CUIDADO: el equipo pedido puede ' +
          'venir SOLO en las imagenes. Extrae de ahi los equipos y cantidades igual que si vinieran en texto. ' +
          'Si una imagen no se entiende, anotalo en "notas".';
        userContent = [{ type: 'text', text: contenido }].concat(bloquesImagen);
      }
      // CAUSA RAIZ MEDIDA (30-jul, log de Render): el intento fallaba con
      // stop_reason=max_tokens y bloques "thinking,text" — el razonamiento
      // adaptativo se comia el presupuesto y truncaba el JSON. Esta extraccion
      // no necesita pensar, asi que en el 1er intento se apaga. El 2o intento va
      // SIN el parametro: si algun dia el modelo no lo acepta, se auto-repara.
      const params = { model: 'claude-opus-5', messages: [{ role: 'user', content: userContent }] }; // Opus: decision de Daniel 26-ago (copiloto muy capaz)
      if (intento === 1) {
        params.max_tokens = 2000;
        params.thinking = { type: 'disabled' };
      } else {
        params.max_tokens = 8000;
      }
      let resp;
      try {
        resp = await anthropic.messages.create(params);
      } catch (e) {
        console.error('[draft-order] error de API en intento ' + intento + ': ' + e.message);
        continue;
      }
      ultimoRaw = claudeText(resp);
      console.log('[draft-order] intento ' + intento + ': stop_reason=' + (resp.stop_reason || '?') +
        ' bloques=' + ((resp.content || []).map(function (b) { return b.type; }).join(',') || 'ninguno') +
        ' chars=' + ultimoRaw.length);
      ext = parseExt(ultimoRaw);
    }
    if (!ext) {
      console.error('[draft-order] JSON ilegible tras 2 intentos. Raw: ' + ultimoRaw.slice(0, 400));
      await draftPostComment(contactId,
        '\ud83e\udd16 No pude armar el borrador: no logre entender la conversacion (fallo tecnico de lectura). ' +
        'Vuelve a apretar el boton, o crea la orden a mano en Booqable. Ya quedo registrado para revisarlo.');
      return res.status(422).json({ ok: false, error: 'no se pudo extraer datos de la conversacion' });
    }
    // Compatibilidad: si el modelo regresa el formato viejo {equipos:[...]},
    // se envuelve como una sola solicitud.
    let solicitudes = Array.isArray(ext.solicitudes) ? ext.solicitudes : null;
    if (!solicitudes && Array.isArray(ext.equipos)) {
      solicitudes = [{
        equipos: ext.equipos, fecha_inicio: ext.fecha_inicio, fecha_regreso: ext.fecha_regreso,
        hora_inicio: ext.hora_inicio, hora_regreso: ext.hora_regreso
      }];
    }
    if (!solicitudes) {
      console.error('[draft-order] extraccion sin solicitudes');
      return res.status(422).json({ ok: false, error: 'extraccion incompleta, reintenta' });
    }
    solicitudes = solicitudes.filter(function (s) {
      return s && Array.isArray(s.equipos) &&
        s.equipos.filter(function (x) { return x && x.descripcion; }).length;
    }).slice(0, 6);
    if (!solicitudes.length) {
      const l = ['\ud83e\udd16 No cree ningun borrador: no hay nada pendiente en esta conversacion.'];
      if (yaPedido.length) {
        l.push('Lo que pidio ya esta levantado:');
        yaPedido.slice(0, 5).forEach(function (y) { l.push('  ' + y); });
        l.push('Si falta algo o hay que moverle a esas ordenes, hazlo en Booqable.');
      } else {
        l.push('No detecte equipo claro que el cliente quiera rentar. Si falta algo, crealo a mano.');
      }
      await draftPostComment(contactId, l.join('\n'));
      return res.json({ ok: false, error: 'sin equipos pendientes detectados', ya_pedido: yaPedido });
    }

    // 3) Helpers de fechas/horas.
    // Booqable guarda la hora VERBATIM con etiqueta +00:00 (igual que las ordenes
    // que el equipo crea a mano): NO convertir a UTC real.
    // Politica Filmorent: recoleccion 9:00; regreso a la MANANA siguiente 9:30;
    // renta del mismo dia regresa 19:00.
    const validaFecha = function (s) { return (s && /^\d{4}-\d{2}-\d{2}$/.test(s)) ? s : null; };
    const validaHora = function (s) {
      if (!s || !/^\d{1,2}:\d{2}$/.test(s)) return null;
      return s.length === 4 ? '0' + s : s;
    };
    const esDomingo = function (s) { return new Date(s + 'T12:00:00Z').getUTCDay() === 0; };
    const manana = new Date(Date.now() + 24 * 3600 * 1000)
      .toLocaleDateString('en-CA', { timeZone: 'America/Monterrey' });

    // 5) Una ORDEN POR SOLICITUD (fechas distintas = ordenes distintas).
    const patchAttrsBase = { tag_list: ['borrador-ai', tagConversacion] };
    if (customerId) patchAttrsBase.customer_id = customerId;
    const resultados = [];
    let patchFallo = false;
    console.log('[draft-order] ordenes vigentes detectadas: ' + ordenesVigentes.length +
      (ordenesVigentes.length ? ' -> ' + ordenesVigentes.map(function (v) { return '#' + v.number + '[' + v.items.length + ']'; }).join(' ') : ''));
    const conflictos = [];
    for (const sol of solicitudes) {
      const equipos = sol.equipos.filter(function (x) { return x && x.descripcion; });
      let fi = validaFecha(sol.fecha_inicio);
      let ff = validaFecha(sol.fecha_regreso);
      const fechasAsumidas = !fi;
      if (!fi) fi = manana;
      const hIni = validaHora(sol.hora_inicio) || '09:00';
      let hReg = validaHora(sol.hora_regreso);
      if (!ff || ff < fi) {
        // Criterio de Barush (28-jul): el regreso normal es a la MANANA SIGUIENTE
        // 9:00-9:30. Solo se queda el mismo dia si el cliente dio hora de regreso
        // (tipico de estudios rentados por horas).
        if (hReg) {
          ff = fi;
        } else {
          ff = new Date(fi + 'T12:00:00Z');
          ff.setUTCDate(ff.getUTCDate() + 1);
          ff = ff.toISOString().slice(0, 10);
          hReg = '09:30';
        }
      }
      if (!hReg) hReg = ff > fi ? '09:30' : '19:00';

      // Resolver productos ANTES de crear nada: asi se puede comparar contra lo
      // que el cliente YA tiene y no se crean ordenes vacias.
      const resueltos = [];
      const noEncontrados = [];
      const ambiguos = [];
      const omitidos = Math.max(0, equipos.length - 12);
      for (const eq of equipos.slice(0, 12)) {
        const qty = Math.max(1, parseInt(eq.cantidad, 10) || 1);
        const hit = await draftFindProduct(eq.descripcion,
          equipos.map(function (x) { return x.descripcion; }));
        if (!hit) { noEncontrados.push(eq.descripcion); continue; }
        if (hit.ambiguo) { ambiguos.push({ pedido: eq.descripcion, opciones: hit.opciones }); continue; }
        resueltos.push({ pedido: eq.descripcion, qty: qty, hit: hit });
      }

      // Choque con una orden existente: fechas que se traslapan + mismo producto.
      // No se decide solo: se le pregunta al equipo (idea de Daniel).
      // Comparar SIN espacios ni acentos: varios productos del catalogo traen
      // espacio al final ("Camara Sony A7IV ") y la linea de la orden viene
      // recortada, asi que la igualdad estricta fallaba y se duplicaba.
      const claveProd = function (s) {
        return String(s || '').trim().toLowerCase().normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '').replace(/\s+/g, ' ');
      };
      const choques = [];
      for (const r of resueltos) {
        const clave = claveProd(r.hit.groupName);
        const ya = ordenesVigentes.find(function (v) {
          const seTraslapan = v.fi <= ff && v.ff >= fi;
          return seTraslapan && v.items.some(function (t) { return claveProd(t) === clave; });
        });
        if (ya) choques.push({ producto: r.hit.groupName, orden: ya });
      }
      if (!resueltos.length && ambiguos.length) {
        // Todo lo que pidio es generico: no se crea orden, se pregunta cual.
        resultados.push({
          orderId: null, number: null, agregados: [], noEncontrados: noEncontrados,
          fallaronReserva: [], ambiguos: ambiguos, omitidos: omitidos, totalPedido: equipos.length,
          fi: fi, ff: ff, hIni: hIni, hReg: hReg, fechasAsumidas: fechasAsumidas,
          sinHora: false, domingo: false, soloPreguntas: true
        });
        continue;
      }
      if (choques.length && choques.length === resueltos.length) {
        conflictos.push({
          fi: fi, ff: ff,
          productos: choques.map(function (c) { return c.producto; }),
          ordenes: Array.from(new Set(choques.map(function (c) { return c.orden.number; })))
        });
        continue; // no se crea nada hasta que el equipo aclare
      }

      // Los estudios se rentan por BLOQUES DE HORAS (Barush): dejarlos de un dia
      // para otro cobra la tarifa de 24h (~$20,000 en vez de ~$4,600). Si el
      // cliente no dio horario, se asume un bloque de 4h y se PREGUNTA.
      const esEstudio = resueltos.some(function (r) { return /estudio/i.test(r.hit.groupName); });
      let horarioEstudioAsumido = false;
      if (esEstudio && !validaHora(sol.hora_regreso)) {
        ff = fi;
        hReg = '13:00';
        horarioEstudioAsumido = true;
      }

      let orderId;
      try {
        const od = await booqableWrite('POST', '/orders', {
          data: {
            type: 'orders',
            attributes: { starts_at: fi + 'T' + hIni + ':00+00:00', stops_at: ff + 'T' + hReg + ':00+00:00' }
          }
        });
        orderId = od.data.id;
      } catch (e) {
        console.error('[draft-order] no se pudo crear la orden: ' + e.message);
        continue;
      }

      const agregados = [];
      const fallaronReserva = [];
      for (const r of resueltos) {
        try {
          const accion = r.hit.esBundle
            ? { action: 'book_bundle', bundle_id: r.hit.bundleId, quantity: r.qty }
            : { action: 'book_product', mode: 'create_new', product_id: r.hit.productId, quantity: r.qty };
          await booqableWrite('POST', '/order_fulfillments', {
            data: { type: 'order_fulfillments', attributes: { order_id: orderId, actions: [accion] } }
          });
          agregados.push(r.pedido + ' -> ' + r.hit.groupName + (r.qty > 1 ? ' x' + r.qty : '') +
            (r.hit.esBundle ? ' [KIT completo]' : '') +
            (r.hit.porHistorial ? ' (el que se pone en el ' + r.hit.porHistorial + '% de estos casos)' : '') +
            (r.hit.porIA ? ' (?? verifica: el nombre no coincidia exacto)' : ''));
        } catch (e) {
          console.error('[draft-order] book ' + r.hit.groupName + ': ' + e.message);
          fallaronReserva.push(r.pedido + ' (' + r.hit.groupName + ')');
        }
      }

      try {
        await booqableWrite('PATCH', '/orders/' + orderId, {
          data: { type: 'orders', id: orderId, attributes: patchAttrsBase }
        });
      } catch (e) {
        patchFallo = true;
        console.error('[draft-order] patch cliente/tag: ' + e.message);
      }

      let orderNumber = null;
      try {
        await booqableWrite('POST', '/order_status_transitions', {
          data: {
            type: 'order_status_transitions',
            attributes: { order_id: orderId, transition_from: 'new', transition_to: 'draft' }
          }
        });
        const chk = await booqableGet('/orders/' + orderId);
        orderNumber = chk.data.attributes.number;
      } catch (e) { console.error('[draft-order] transicion a draft: ' + e.message); }

      // Nota EN la orden (Booqable): el contexto no puede vivir solo en Respond.io — quien
      // abra la orden debe entender que tiene y que falto (caso #10801: quedo vacia y sin explicacion).
      try {
        const notaL = ['Creada por el copiloto AI desde la conversacion de WhatsApp. NADA se envio al cliente.'];
        if (agregados.length) notaL.push('Equipo agregado: ' + agregados.join(' | '));
        if (noEncontrados.length) notaL.push('NO ENCONTRADO en catalogo (agregar a mano): ' + noEncontrados.join(', '));
        if (fallaronReserva.length) notaL.push('Fallo la reserva (agregar a mano): ' + fallaronReserva.join(', '));
        if (bloquesImagen.length) notaL.push('El cliente mando ' + bloquesImagen.length + ' imagen(es) con su lista; se leyeron con AI — verificar contra el chat.');
        if (ext && ext.notas) notaL.push('Notas del chat: ' + String(ext.notas).slice(0, 300));
        await booqableWrite('POST', '/notes', {
          data: { type: 'notes', attributes: { body: notaL.join('\n'), owner_id: orderId, owner_type: 'orders' } }
        });
      } catch (e) { console.error('[draft-order] nota en orden: ' + e.message); }

      resultados.push({
        orderId: orderId, number: orderNumber, agregados: agregados, noEncontrados: noEncontrados,
        fallaronReserva: fallaronReserva, omitidos: omitidos, totalPedido: equipos.length,
        parcialmenteRepetida: choques.length ? choques.map(function (c) { return c.producto + ' ya esta en #' + c.orden.number; }) : [],
        ambiguos: ambiguos,
        fi: fi, ff: ff, hIni: hIni, hReg: hReg, fechasAsumidas: fechasAsumidas,
        sinHora: !validaHora(sol.hora_inicio) && !fechasAsumidas,
        horarioEstudioAsumido: horarioEstudioAsumido,
        domingo: esDomingo(fi) || esDomingo(ff)
      });
    }

    // Si TODO lo que pidio ya existe, no se crea nada: se pregunta.
    if (!resultados.length && conflictos.length) {
      const l = ['\ud83e\udd16 NO cree ningun borrador para evitar duplicados.'];
      conflictos.forEach(function (c) {
        l.push('Ya existe la orden #' + c.ordenes.join(', #') + ' con ' + c.productos.join(', ') +
          ' en esas mismas fechas (' + c.fi + ' a ' + c.ff + ').');
      });
      l.push('');
      l.push('PREGUNTALE AL CLIENTE (copia y pega):');
      l.push('  Ya tienes una orden apartada para esas fechas con lo mismo. \u00bfEs una orden NUEVA, ' +
        'es la misma, o quieres que le movamos a la que ya tienes?');
      l.push('');
      l.push('Si es NUEVA: duplica la orden en Booqable o crea otra a mano.');
      await draftPostComment(contactId, l.join('\n'));
      return res.json({ ok: true, ordenes: [], conflictos: conflictos });
    }

    if (!resultados.length) {
      await draftPostComment(contactId,
        '\ud83e\udd16 No pude crear el borrador en Booqable (error al crear la orden). Hazlo a mano por favor.');
      return res.status(502).json({ ok: false, error: 'no se pudo crear ninguna orden' });
    }

    // 6) UN comentario con todo. Incluye aviso de posible duplicado.
    // El comentario se arma en dos partes y las PREGUNTAS van HASTA ARRIBA:
    // Respond.io colapsa los comentarios largos con "Show more" y lo que queda
    // abajo no se ve (Daniel: "hizo la orden pero sin preguntarme a que nombre"
    // — si preguntaba, pero escondido).
    const preguntas = [];
    const cabeza = [];
    const detalle = [];
    const previo = draftRecientes[contactId];
    const ahora = Date.now();
    cabeza.push(resultados.length === 1
      ? '🤖 BORRADOR ' + (resultados[0].number ? '#' + resultados[0].number : '(sin numero)') +
        ' creado. NADA se envio al cliente.'
      : '🤖 ' + resultados.length + ' BORRADORES creados (fechas distintas = ordenes distintas). ' +
        'NADA se envio al cliente.');
    draftRecientes[contactId] = {
      at: ahora,
      numbers: resultados.map(function (r) { return r.number || '?'; })
    };

    let lineaCliente;
    if (customerName && !patchFallo) {
      lineaCliente = 'Cliente: ' + customerName +
        (clienteAprendido
          ? ' (el mismo que ustedes asignaron en una orden anterior de este chat).'
          : ' (' + (candidatos[0] ? candidatos[0].ordenes : '?') + ' rentas previas). Confirma que va a ESTE cliente antes de enviar.');
    } else if (customerName && patchFallo) {
      lineaCliente = '⚠️ Cliente: NO SE PUDO ASIGNAR por un error tecnico. Deberia ser "' +
        customerName + '" — asignalo a mano (y ponle el tag borrador-ai).';
    } else if (candidatos.length >= 1) {
      // Con link directo a cada candidato: asignarlo es un clic, no una busqueda.
      const linkCliente = function (c) {
        return '  \u2022 ' + c.name + ' (' + c.ordenes + ' rentas, ultima ' + (c.ultima || 's/f') + ')' +
          '\n    https://filmorent-sa-de-cv.booqable.com/customers/' + c.id + '/edit'; // sin /edit sale en blanco
      };
      if (candidatos.length > 1) {
        lineaCliente = 'Cliente: SIN ASIGNAR — este contacto tiene ' + candidatos.length +
          ' registros en Booqable, abre el que va y asignalo:\n' +
          candidatos.slice(0, 3).map(linkCliente).join('\n');
        preguntas.push('¿La orden va a nombre de ' + candidatos[0].name + ' o de ' + candidatos[1].name + '?');
      } else {
        lineaCliente = 'Cliente: SIN ASIGNAR' +
          (candidatos[0].porTelefono ? ' (su telefono coincide pero el nombre no; puede ser su empresa)' : '') +
          '. Unico parecido:\n' + linkCliente(candidatos[0]);
        preguntas.push('¿La orden va a nombre de ' + candidatos[0].name + '?');
      }
    } else {
      lineaCliente = 'Cliente: SIN ASIGNAR, no lo encontre en Booqable' +
        (nombreContacto ? ' (en WhatsApp se llama "' + nombreContacto + '")' : '') + '. Crealo o buscalo tu.';
      preguntas.push('¿A nombre de quien facturamos la orden?');
    }

    for (const r of resultados) {
      detalle.push('');
      detalle.push((r.number ? '#' + r.number : (r.soloPreguntas ? '(NO se creo orden, falta definir el equipo)' : '(sin numero)')) +
        ' — recoge ' + r.fi + ' ' + r.hIni +
        ', regresa ' + r.ff + ' ' + r.hReg + (r.fechasAsumidas ? ' (FECHAS ASUMIDAS)' : ''));
      if (r.agregados.length) detalle.push('  Equipo: ' + r.agregados.join(' | '));
      if (r.parcialmenteRepetida && r.parcialmenteRepetida.length) {
        detalle.push('  ⚠️ Repetido: ' + r.parcialmenteRepetida.join('; '));
        preguntas.push('Ya tienes apartado ' + r.parcialmenteRepetida[0].split(' ya esta')[0] +
          ' para esas fechas. ¿Es una orden NUEVA, es la misma, o le movemos a la que ya tienes?');
      }
      if (r.ambiguos && r.ambiguos.length) {
        r.ambiguos.forEach(function (a) {
          detalle.push('  ⚠️ "' + a.pedido + '" es muy general, hay varios: ' + a.opciones.join(' / ') +
            '. NO elegi ninguno, agregalo tu.');
          preguntas.push('Sobre el ' + a.pedido + ': manejamos varias opciones (' +
            a.opciones.slice(0, 3).join(', ') + '). ¿Cual necesitas o para que lo vas a usar?');
        });
      }
      if (r.noEncontrados.length) detalle.push('  ⚠️ NO encontre en catalogo: ' + r.noEncontrados.join(', '));
      if (r.fallaronReserva.length) detalle.push('  ⚠️ Encontrados pero NO agregados: ' + r.fallaronReserva.join(', '));
      if (r.omitidos > 0) detalle.push('  ⚠️ Pidio ' + r.totalPedido + ' equipos; solo procese 12.');
      if (r.horarioEstudioAsumido) {
        detalle.push('  ⚠️ ESTUDIO sin horario: asumi 4 horas (9:00-13:00). El precio cambia mucho segun las horas.');
        preguntas.push('¿De que hora a que hora ocupas el estudio el ' + r.fi + '? (manejamos bloques de 2, 4, 8 o 12 horas)');
      } else if (r.sinHora) {
        detalle.push('  ⚠️ Sin hora de recoleccion (se asumio 9:00).');
        preguntas.push('¿A que hora pasas por el equipo el ' + r.fi + '? ¿Y que dia lo regresas?');
      }
      if (r.agregados.some(function (a) { return /estudio/i.test(a); })) {
        // Medido 31-jul: el Filmo Grand tiene 4 tarifas por duracion (Estudio,
        // Modulo extra, All access, Montaje) y al reservar por API Booqable toma
        // "Montaje", la mas barata ($2,300 en vez de $4,600 a 4 horas). El
        // price_tile_id no se deja cambiar por API (el PATCH pasa y se ignora),
        // asi que se marca para que un humano elija la tarifa correcta.
        detalle.push('  \u26a0\ufe0f PRECIO DEL ESTUDIO MAL: Booqable puso la tarifa "Montaje" (la mas barata). ' +
          'Cambia la tarifa en la linea a "Estudio N Horas", "Modulo extra" o "All access" segun lo que ocupe.');
      }
      if (r.domingo) {
        // NO decirle al cliente que cerramos: si se abre en domingo, con cargo de
        // encargado (dato de Daniel, 30-jul; pendiente confirmar monto con el equipo).
        detalle.push('  \u26a0\ufe0f Cae en DOMINGO: confirma disponibilidad y si aplica cargo de encargado.');
      }
      if (r.orderId) detalle.push('  https://filmorent-sa-de-cv.booqable.com/orders/' + r.orderId);
    }

    const hayEstudio = resultados.some(function (r) {
      return r.agregados.some(function (a) { return /estudio/i.test(a); });
    });
    if (hayEstudio) {
      preguntas.push('¿Cuantas personas van a estar? ¿Necesitas sala adicional o solo el estudio?');
    }

    const lineas = cabeza.slice();
    // Sin repetidas: con varias ordenes la misma pregunta salia 2-3 veces
    // (ej. "el domingo estamos cerrados" por cada orden de ese dia).
    const preguntasUnicas = preguntas.filter(function (q, i) { return preguntas.indexOf(q) === i; });
    if (preguntasUnicas.length) {
      lineas.push('');
      lineas.push('❓ FALTA PREGUNTARLE AL CLIENTE (copia y pega):');
      preguntasUnicas.slice(0, 5).forEach(function (q) { lineas.push('  ' + q); });
    }
    lineas.push('');
    lineas.push(lineaCliente);
    if (previo && (ahora - previo.at) < 6 * 3600 * 1000) {
      lineas.push('⚠️ Hace ' + Math.round((ahora - previo.at) / 60000) + ' min ya se habia creado ' +
        (previo.numbers.length > 1 ? 'los borradores #' : 'el borrador #') + previo.numbers.join(', #') +
        ' para este contacto.');
    }
    Array.prototype.push.apply(lineas, detalle);
    if (ext.notas) { lineas.push(''); lineas.push('Notas: ' + String(ext.notas).slice(0, 160)); }
    await draftPostComment(contactId, lineas.join('\n'));

    console.log('[draft-order] ' + resultados.length + ' orden(es) ' +
      resultados.map(function (r) { return r.number; }).join(',') + ' para contacto ' + contactId);
    return res.json({
      ok: true,
      ordenes: resultados.map(function (r) {
        return {
          number: r.number, order_id: r.orderId, agregados: r.agregados,
          no_encontrados: r.noEncontrados, fallaron_reserva: r.fallaronReserva,
          recoge: r.fi + ' ' + r.hIni, regresa: r.ff + ' ' + r.hReg, fechas_asumidas: r.fechasAsumidas
        };
      }),
      cliente: customerName || null,
      cliente_aprendido: clienteAprendido,
      candidatos: candidatos,
      preguntas: preguntasUnicas,
      duplicado_de: previo && (ahora - previo.at) < 6 * 3600 * 1000 ? previo.numbers : null
    });
  } catch (e) {
    console.error('[draft-order] error: ' + (e && e.message));
    // Nunca dejar al empleado esperando sin saber que paso.
    try {
      const cid = extractContactId(req.body);
      if (cid && /^\d+$/.test(String(cid))) {
        await draftPostComment(cid,
          '\ud83e\udd16 No pude crear el borrador por un error tecnico. Crea la orden a mano en Booqable ' +
          'o vuelve a intentar en un minuto. (Detalle: ' + String((e && e.message) || e).slice(0, 120) + ')');
      }
    } catch (e2) { /* ni modo */ }
    return res.status(500).json({ ok: false, error: String((e && e.message) || e).slice(0, 300) });
  }
});



// =====================================================================
// ENVIAR INFO DEL ESTUDIO CON FOTOS (idea de Daniel, 3-ago-2026)
// POST /webhook/enviar-info  body: { contactId }
// El AI contestaba con puros links. Esto manda el cuadro de precios
// publicado en filmorent.com (verificado: sus 15 precios coinciden con
// Booqable y con el KB) y fotos reales del estudio. Detecta solo si
// preguntan por el Grand o el Pocket leyendo la conversacion.
// Lo dispara un HUMANO desde un Shortcut: el bot no manda esto solo.
// =====================================================================
// Secuencia de lo que se le manda al cliente. {t: texto} o {i: url de imagen}.
// Respond.io DESCARTA el pie de foto (probado con description y caption: pasa
// 200 pero no sobrevive), asi que cada foto lleva su nombre en una linea antes.
// Los textos los LEE EL CLIENTE: van con acentos y enyes de verdad.
const INFO_ESTUDIOS = {
  grand: {
    titulo: 'Filmo Grand',
    secuencia: [
      { t: 'FILMO GRAND \u2014 el grande, para producciones, videos musicales, comerciales y hasta autos. ' +
           'Ciclorama, cocina, ba\u00f1o, mobiliario y pantalla de 65".\nhttps://filmorent.com/estudio-filmo-grand/' },
      { i: 'https://filmorent.com/wp-content/uploads/estudioinicio.jpg' },
      { t: 'Estos son los tres paquetes. El bono de equipo es el 50% de lo que pagas de estudio, ' +
           'para gastarlo en renta de equipo:' },
      { i: 'https://filmorent.com/wp-content/uploads/filmogrand_precios_2026.jpeg' },
      { t: 'Estas son las medidas:' },
      { i: 'https://filmorent-tag-analyzer.onrender.com/assets/medidas-filmo-grand.jpg' },
      { t: 'Y estos son los m\u00f3dulos que puedes agregar \u2014 sala de maquillaje, cocina y sala de ' +
           'clientes con vista al estudio:' },
      { i: 'https://filmorent.com/wp-content/uploads/estudio-filmogrand-2.jpeg' },
      { i: 'https://filmorent.com/wp-content/uploads/estudio-filmogrand-1.jpeg' },
      { i: 'https://filmorent.com/wp-content/uploads/estudio-filmogrand-3.jpeg' }
    ]
  },
  pocket: {
    titulo: 'Filmo Pocket',
    secuencia: [
      // NO mencionar que esta en el tercer piso: es un dato logistico que solo
      // desincentiva la venta (Daniel, 7-ago). Se resuelve al agendar.
      { t: 'FILMO POCKET \u2014 el compacto. Ideal para photoshoots, podcast, contenido y videos ' +
           'musicales de escala chica. Sale en $700 por hora.\nhttps://filmorent.com/estudio-filmo-pocket/' },
      { i: 'https://filmorent.com/wp-content/uploads/estudio-pocket-reservacion.jpg' },
      { t: 'Estas son sus medidas:' },
      { i: 'https://filmorent.com/wp-content/uploads/estudio-pocket-medidas.jpg' }
    ]
  }
};

async function respondioEnviar(contactId, channelId, cuerpo) {
  const r = await fetch('https://api.respond.io/v2/contact/id:' + contactId + '/message', {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify(Object.assign({ channelId: channelId }, cuerpo))
  });
  if (!r.ok) throw new Error('Respond.io ' + r.status + ': ' + (await r.text()).slice(0, 160));
  return r.json();
}

// WhatsApp y Messenger no renderizan .webp: llega como link y se ve mal.
function draftImagenEnviable(url) {
  return /\.(jpe?g|png)(\?|$)/i.test(String(url || ''));
}

app.post('/webhook/enviar-info', async (req, res) => {
  try {
    const contactId = extractContactId(req.body);
    if (!contactId || !/^\d+$/.test(String(contactId))) {
      return res.status(400).json({ ok: false, error: 'contactId faltante o invalido' });
    }
    const md = await draftRespondioGet('/contact/id:' + contactId + '/message/list?limit=25');
    const msgs = (md.items || md.data || []);
    // SOLO mensajes del CLIENTE. Antes se leia toda la conversacion, incluidos
    // NUESTROS propios mensajes — y como el texto del Grand dice "ciclorama" y
    // "cocina", al segundo apreton el detector creia que habian pedido el Grand
    // y el Pocket ya nunca salia (Daniel, 7-ago).
    const texto = msgs.filter(function (m) { return m.traffic === 'incoming'; })
      .map(function (m) {
        const x = m.message || {};
        return String(x.text || x.message || '');
      }).join(' ').toLowerCase();
    const canal = (msgs[0] && msgs[0].channelId) || null;
    // Casi nadie dice CUAL estudio, solo "el estudio" (Daniel, 7-ago). Adivinar
    // el Grand deja fuera al Pocket, que es la opcion barata. Si no lo dicen
    // claro, se mandan LOS DOS y que el cliente elija.
    const pidePocket = /pocket|chico|peque|podcast/.test(texto);
    const pideGrand = /grand|grande|ciclorama|cocina/.test(texto);
    const cuales = (pidePocket && !pideGrand) ? ['pocket']
      : (pideGrand && !pidePocket) ? ['grand']
      : ['grand', 'pocket'];

    if (cuales.length === 2) {
      await respondioEnviar(contactId, canal, {
        message: { type: 'text', text: 'Con gusto. Manejamos dos estudios y te paso los dos para que ' +
          'veas cu\u00e1l te acomoda mejor:' }
      });
    }
    const enviadas = [];
    const omitidas = [];
    for (const cual of cuales) {
      for (const paso of INFO_ESTUDIOS[cual].secuencia) {
        if (paso.t) {
          await respondioEnviar(contactId, canal, { message: { type: 'text', text: paso.t } });
          continue;
        }
        if (!draftImagenEnviable(paso.i)) { omitidas.push(paso.i.split('/').pop()); continue; }
        try {
          await respondioEnviar(contactId, canal, {
            message: { type: 'attachment', attachment: { type: 'image', url: paso.i } }
          });
          enviadas.push(paso.i.split('/').pop());
        } catch (e) {
          console.error('[enviar-info] imagen ' + paso.i + ': ' + e.message);
          omitidas.push(paso.i.split('/').pop());
        }
      }
    }
    await respondioEnviar(contactId, canal, {
      message: { type: 'text', text: '\u00bfQu\u00e9 d\u00eda y de qu\u00e9 hora a qu\u00e9 hora lo ocupas? ' +
        'Con eso te checo disponibilidad y te paso el total.' }
    });
    await draftPostComment(contactId,
      '\ud83d\udcf8 Le mande info de: ' + cuales.map(function (c) { return INFO_ESTUDIOS[c].titulo; }).join(' y ') +
      ' (' + enviadas.length + ' imagenes)' +
      (cuales.length === 2 ? ' \u2014 no dijo cual queria, asi que fueron los dos.' : '.') +
      (omitidas.length ? ' NO se pudieron mandar: ' + omitidas.join(', ') + '.' : '') +
      ' Falta que confirme dia y horario.');
    console.log('[enviar-info] ' + cuales.join('+') + ' -> contacto ' + contactId +
      ' (' + enviadas.length + ' imagenes, ' + omitidas.length + ' omitidas)');
    return res.json({ ok: true, estudios: cuales, imagenes: enviadas, omitidas: omitidas });
  } catch (e) {
    console.error('[enviar-info] ' + (e && e.message));
    return res.status(500).json({ ok: false, error: String((e && e.message) || e).slice(0, 200) });
  }
});


// ============================================================
// ORDENES v8.42.0 — PDF propio + liga de pago SIN CLICKS (26-ago-2026)
// Elimina los 2 unicos pasos manuales del ciclo de ordenes:
//  GET  /orden/:uuid/proforma.pdf  -> Orden de Servicio en PDF, generada AQUI
//       (URL limpia y estable; el uuid de Booqable es el secreto — mismo modelo que /pay/<uuid>)
//  POST /orden/:uuid/liga          -> crea (o reusa) la liga de pago por el saldo
//       via POST /api/boomerang/payment_charges (descubierto 26-ago: el path /api/4 da 404
//       pero boomerang acepta {order_id, mode:"request", amount_in_cents} con el API key)
// ============================================================
const PDFDocument = require('pdfkit');

const BOOQ_HOST = 'https://filmorent-sa-de-cv.booqable.com';

async function ordenPorUuid(uuid) {
  const od = await booqableGet('/orders/' + uuid + '?include=customer');
  const a = od.data.attributes;
  const inc = (od.included || []).find(x => x.type === 'customers');
  const cliente = inc ? inc.attributes : null;
  const ld = await booqableGet('/lines?filter[order_id]=' + uuid + '&page[size]=60');
  // Solo lineas de PRIMER NIVEL: los componentes de un kit traen parent_line_id y ya van
  // incluidos en el precio del kit — listarlos duplica visualmente el total (bug detectado 26-ago).
  const lineas = (ld.data || [])
    .map(l => l.attributes)
    .filter(l => !l.archived && !l.parent_line_id && (l.line_type === 'charge' || (l.price_in_cents || 0) < 0))
    .sort((x, y) => (x.position || 0) - (y.position || 0));
  return { a, cliente, lineas };
}

const pdfMXN = c => '$' + ((c || 0) / 100).toLocaleString('es-MX', { minimumFractionDigits: 2 }) + ' MXN';
const pdfFecha = iso => { if (!iso) return '-'; const d = new Date(iso);
  return d.toLocaleDateString('es-MX', { timeZone: 'UTC', day: '2-digit', month: 'short', year: 'numeric' }) + ' ' +
         d.toLocaleTimeString('es-MX', { timeZone: 'UTC', hour: '2-digit', minute: '2-digit', hour12: true }); };

async function generarOrdenPDF(req, res, nombreArchivo) {
  try {
    const uuid = String(req.params.uuid || '');
    if (!/^[0-9a-f-]{36}$/.test(uuid)) return res.status(400).send('uuid invalido');
    let orden;
    try { orden = await ordenPorUuid(uuid); } catch (e) { return res.status(404).send('orden no encontrada'); }
    const { a, cliente, lineas } = orden;

    const archivo = nombreArchivo || ('Orden-' + (a.number || 'servicio') + '-Filmorent.pdf');
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'inline; filename="' + archivo + '"');
    const doc = new PDFDocument({ size: 'LETTER', margin: 50 });
    doc.pipe(res);

    // Encabezado
    doc.fontSize(20).fillColor('#8B1E1E').font('Helvetica-Bold').text('FILMORENT', 50, 50);
    doc.fontSize(9).fillColor('#444').font('Helvetica')
      .text('Filmorent SA de CV', 50, 74)
      .text('Blvd Rogelio Cantu Gomez #333 L8, Col. Santa Maria', 50, 86)
      .text('64650 Monterrey, Nuevo Leon, Mexico', 50, 98)
      .text('info@filmorent.com  ·  WhatsApp: +52 811 582 2788', 50, 110);
    doc.fontSize(16).fillColor('#111').font('Helvetica-Bold').text('Orden de Servicio', 380, 50, { align: 'right' });
    doc.fontSize(11).font('Helvetica').fillColor('#111').text('#' + (a.number || 's/n'), 380, 72, { align: 'right' });
    doc.fontSize(9).fillColor('#444')
      .text('Recoge:  ' + pdfFecha(a.starts_at), 340, 90, { align: 'right' })
      .text('Regresa: ' + pdfFecha(a.stops_at), 340, 102, { align: 'right' });

    // Cliente
    doc.moveTo(50, 130).lineTo(562, 130).strokeColor('#DDD').stroke();
    doc.fontSize(9).fillColor('#888').text('CLIENTE', 50, 140);
    doc.fontSize(11).fillColor('#111').font('Helvetica-Bold')
      .text((cliente && cliente.name) ? String(cliente.name).split('/')[0].trim() : 'Por confirmar', 50, 152);
    if (cliente && cliente.email) doc.fontSize(9).font('Helvetica').fillColor('#444').text(cliente.email, 50, 168);

    // Tabla (desglosada, pedido de Daniel 27-ago: tarifa por linea, como los PDFs del equipo)
    const esLbl = l => String(l || '-').replace(/\bdays\b/, 'dias').replace(/\bday\b/, 'dia');
    let y = 195;
    doc.fontSize(8).fillColor('#888').font('Helvetica-Bold');
    doc.text('CANT', 50, y); doc.text('EQUIPO', 85, y); doc.text('TARIFA', 355, y); doc.text('TOTAL', 490, y, { width: 72, align: 'right' });
    y += 14; doc.moveTo(50, y).lineTo(562, y).strokeColor('#DDD').stroke(); y += 8;
    doc.font('Helvetica').fontSize(9);
    for (const l of lineas) {
      if (y > 690) { doc.addPage(); y = 60; }
      const esDesc = (l.price_in_cents || 0) < 0;
      doc.fillColor(esDesc ? '#0A7A38' : '#111');
      doc.text(String(l.quantity || 1) + 'x', 50, y, { width: 30 });
      doc.text(String(l.title || '').slice(0, 60), 85, y, { width: 262 });
      doc.text(pdfMXN(l.price_each_in_cents || l.price_in_cents).replace(' MXN', '') + ' x ' + esLbl(l.charge_label), 355, y, { width: 110 });
      doc.text(pdfMXN(l.price_in_cents), 470, y, { width: 92, align: 'right' });
      y += Math.max(14, Math.ceil(String(l.title || '').length / 52) * 12) + 4;
    }
    y += 6; doc.moveTo(50, y).lineTo(562, y).strokeColor('#DDD').stroke(); y += 10;

    // Desglose de totales (como pro-forma de Booqable): suma -> descuento -> sin IVA -> IVA -> total
    const totalConIva = a.grand_total_with_tax_in_cents || 0;
    const sinIva = a.grand_total_in_cents || 0;
    const iva = a.tax_in_cents != null ? a.tax_in_cents : (totalConIva - sinIva);
    const descuento = (a.total_discount_in_cents || 0);
    const sumaLineas = lineas.reduce((t, l) => t + (l.price_in_cents || 0), 0);
    const fila = (label, val, opts) => {
      opts = opts || {};
      doc.font(opts.bold ? 'Helvetica-Bold' : 'Helvetica').fontSize(opts.big ? 10 : 9).fillColor(opts.color || '#444');
      doc.text(label, 300, y, { width: 160 }); doc.text(val, 460, y, { width: 102, align: 'right' });
      y += opts.big ? 16 : 13;
    };
    if (descuento >= 100) {
      fila('Suma equipo (IVA incluido)', pdfMXN(sumaLineas));
      fila('Descuento aplicado', '-' + pdfMXN(descuento), { color: '#0A7A38', bold: true });
    }
    fila('Subtotal (sin IVA)', pdfMXN(sinIva));
    fila('IVA', pdfMXN(iva));
    fila('TOTAL (IVA incluido)', pdfMXN(totalConIva), { bold: true, big: true, color: '#111' });
    const pagado = a.total_paid_in_cents || 0;
    if (pagado > 0) {
      fila('Pagado', pdfMXN(pagado));
      fila('Saldo', pdfMXN(totalConIva - pagado), { bold: true, color: '#8B1E1E' });
    }
    if ((a.deposit_in_cents || 0) > 0) fila('Deposito en garantia', pdfMXN(a.deposit_in_cents));

    y += 14;
    doc.fontSize(8).font('Helvetica').fillColor('#666').text(
      'Precios con IVA incluido. Recoleccion presencial con identificacion oficial fisica. ' +
      'Recoger sin costo un dia antes (L-V 3:30-7:00 pm). Devolucion sin costo L-V 9:00-11:30 am, sabado 9:30-11:30 am. ' +
      'Equipo sujeto a disponibilidad hasta confirmar. Cancelacion de ultimo momento genera cargo del 20%.',
      50, y, { width: 512 });
    doc.end();
  } catch (e) {
    console.error('[orden-pdf] ' + e.message);
    if (!res.headersSent) res.status(500).send('error generando pdf');
  }
}
// URL con el numero de orden en el NOMBRE del archivo (respond.io/WhatsApp nombran el adjunto
// por el ultimo segmento de la URL — pedido de Daniel 27-ago). /proforma.pdf queda por compatibilidad.
app.get('/orden/:uuid/proforma.pdf', (req, res) => generarOrdenPDF(req, res, null));
app.get('/orden/:uuid/:archivo', (req, res, next) => {
  const archivo = String(req.params.archivo || '');
  if (!/^Orden-[\w.()-]+\.pdf$/i.test(archivo)) return next();
  return generarOrdenPDF(req, res, archivo);
});

app.post('/orden/:uuid/liga', async (req, res) => {
  try {
    if (DRAFT_ORDER_TOKEN && (req.headers['x-draft-token'] || '') !== DRAFT_ORDER_TOKEN) {
      return res.status(401).json({ ok: false, error: 'token invalido' });
    }
    const uuid = String(req.params.uuid || '');
    if (!/^[0-9a-f-]{36}$/.test(uuid)) return res.status(400).json({ ok: false, error: 'uuid invalido' });
    let od; try { od = await booqableGet('/orders/' + uuid); } catch (e) { return res.status(404).json({ ok: false, error: 'orden no encontrada' }); }
    const a = od.data.attributes;
    const saldo = (a.grand_total_with_tax_in_cents || 0) - (a.total_paid_in_cents || 0);
    const monto = parseInt((req.body || {}).amount_in_cents, 10) || saldo;
    if (monto < 1000) return res.status(422).json({ ok: false, error: 'monto minimo $10 MXN' });
    // reusar una liga viva del mismo monto (no acumular basura)
    try {
      const pd = await booqableGet('/payments?filter[order_id]=' + uuid + '&page[size]=20');
      const viva = (pd.data || []).find(p => p.attributes.mode === 'request' &&
        p.attributes.status === 'created' && p.attributes.amount_in_cents === monto);
      if (viva) return res.json({ ok: true, reused: true, url: 'https://filmorent-sa-de-cv.booqableshop.com/pay/' + viva.id, amount_in_cents: monto, order_number: a.number });
    } catch (e) {}
    const r = await fetch(BOOQ_HOST + '/api/boomerang/payment_charges', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + BOOQABLE_API_KEY, 'Content-Type': 'application/vnd.api+json' },
      body: JSON.stringify({ data: { type: 'payment_charges', attributes: { order_id: uuid, mode: 'request', amount_in_cents: monto } } }),
    });
    const body = await r.json().catch(() => ({}));
    if (r.status !== 201 || !body.data) {
      console.error('[orden-liga] HTTP ' + r.status + ': ' + JSON.stringify(body).slice(0, 200));
      return res.status(502).json({ ok: false, error: 'no se pudo crear la liga (HTTP ' + r.status + ')' });
    }
    return res.json({ ok: true, reused: false, url: 'https://filmorent-sa-de-cv.booqableshop.com/pay/' + body.data.id, amount_in_cents: monto, order_number: a.number });
  } catch (e) {
    console.error('[orden-liga] ' + e.message);
    return res.status(500).json({ ok: false, error: 'error interno' });
  }
});


app.listen(PORT, () => {
  console.log('Filmorent Tag Analyzer v7.2.1 running on port ' + PORT);
  console.log('Whisper transcription: ' + (openai ? 'ENABLED' : 'DISABLED (set OPENAI_API_KEY to enable)'));
  console.log('Auto-summary on conversation opened: ENABLED');
  console.log('Draft-order copilot (/webhook/draft-order): ' + (BOOQABLE_API_KEY ? ('ENABLED, token ' + (DRAFT_ORDER_TOKEN ? 'ON' : 'OFF')) : 'DISABLED (set BOOQABLE_API_KEY)'));
  console.log('Rewards endpoints (/rewards/*): ' + (BOOQABLE_API_KEY ? 'ENABLED' : 'DISABLED (set BOOQABLE_API_KEY)') + (REWARDS_SHEETS_URL ? ', ledger ON' : ', ledger OFF'));
});
