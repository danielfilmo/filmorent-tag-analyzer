const express = require('express');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();
app.use(express.json());

// Config
const RESPONDIO_API_KEY = process.env.RESPONDIO_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const PORT = process.env.PORT || 3000;

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok' }));

/**
 * Extract contact ID from Respond.io webhook payload.
 */
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

// Main webhook endpoint
app.post('/webhook/conversation-closed', async (req, res) => {
  console.log('\n[' + new Date().toISOString() + '] === WEBHOOK RECEIVED ===');
  console.log('Headers:', JSON.stringify(req.headers, null, 2));
  console.log('Body:', JSON.stringify(req.body, null, 2));

  res.json({ received: true });

  const contactId = extractContactId(req.body);
  const contactName = extractContactName(req.body);

  if (!contactId) {
    console.error('Could not extract contact_id from payload.');
    return;
  }

  console.log('Analyzing conversation for contact: ' + contactId + ' (' + contactName + ')');

  try {
    const messagesResponse = await fetch(
      'https://api.respond.io/v2/contact/id:' + contactId + '/message/list?limit=50',
      {
        headers: {
          'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
          'Content-Type': 'application/json'
        }
      }
    );

    if (!messagesResponse.ok) {
      const errorText = await messagesResponse.text();
      console.error('Respond.io API error: ' + messagesResponse.status + ' - ' + errorText);
      return;
    }

    const messagesData = await messagesResponse.json();
    const messages = messagesData.data || messagesData.items || [];

    if (messages.length === 0) {
      console.log('No messages found for this contact');
      return;
    }

    // Format messages for Claude analysis
    const formattedMessages = messages.map(msg => {
      const sender = msg.type === 'incoming' ? 'CLIENTE' : 'AGENTE';
      const text = msg.text || msg.message?.text || msg.body || '[media/attachment]';
      return sender + ': ' + text;
    }).join('\n');

    // Analyze with Claude
    const analysisPrompt = 'Analiza la siguiente conversacion de un negocio de RENTA de equipo de cine y fotografia (Filmorent, Monterrey Mexico). Determina si aplica alguno de estos tags:\n\n1. "consulta-compra" - El cliente pregunta por COMPRAR equipo (no rentar). Filmorent solo renta, no vende.\n2. "equipo-no-disponible" - El cliente pregunta por equipo que probablemente NO esta en el catalogo de renta (equipo muy especializado, marcas no comunes, etc).\n3. "incidencia" - El cliente reporta un problema, queja, equipo danado, entrega tarde, cobro incorrecto o cualquier situacion negativa.\n\nCONVERSACION:\n' + formattedMessages + '\n\nResponde UNICAMENTE con un JSON valido en este formato exacto, sin texto adicional:\n{"tags": ["tag1", "tag2"]}\n\nSi no aplica ningun tag, responde: {"tags": []}\nSolo incluye tags que CLARAMENTE apliquen basado en la conversacion.';

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 150,
      messages: [{ role: 'user', content: analysisPrompt }]
    });

    const analysisText = claudeResponse.content[0].text.trim();
    console.log('Claude analysis: ' + analysisText);

    let tagsToApply = [];
    try {
      const parsed = JSON.parse(analysisText);
      tagsToApply = parsed.tags || [];
    } catch (e) {
      const validTags = ['consulta-compra', 'equipo-no-disponible', 'incidencia'];
      validTags.forEach(tag => {
        if (analysisText.toLowerCase().includes(tag)) {
          tagsToApply.push(tag);
        }
      });
    }

    const validTags = ['consulta-compra', 'equipo-no-disponible', 'incidencia'];
    tagsToApply = tagsToApply.filter(tag => validTags.includes(tag));

    // Apply tags via Respond.io API
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
        console.log('Tags applied successfully: ' + tagsToApply.join(', '));
      }
    } else {
      console.log('No tags to apply');
    }

    console.log('=== DONE: contact=' + contactId + ', tags=' + JSON.stringify(tagsToApply) + ' ===\n');

  } catch (error) {
    console.error('Error:', error.message);
  }
});

app.listen(PORT, () => {
  console.log('Filmorent Tag Analyzer running on port ' + PORT);
});
