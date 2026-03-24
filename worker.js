/**
 * Cloudflare Worker — receives Telegram webhook, triggers GitHub Actions.
 *
 * Commands:
 *   Send file(s)       — bot saves them to KV (with optional caption as context note)
 *   /ramp ASSET        — generate report using any staged files + asset
 *   /clear             — clear staged files
 *   /status            — show staged files
 *
 * Environment variables (set in Cloudflare Worker settings):
 *   BOT_TOKEN       — Telegram bot token
 *   GITHUB_TOKEN    — GitHub personal access token (repo scope)
 *   GITHUB_REPO     — e.g. "yourusername/ramp-bot"
 *   ALLOWED_CHAT_ID — your Telegram chat ID (233058647)
 *
 * KV Binding (set in Cloudflare Worker settings):
 *   RAMP_KV — KV namespace for staging files between messages
 */

export default {
  async fetch(request, env) {
    if (request.method !== "POST") return new Response("ok");

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("ok");
    }

    const message = body.message;
    if (!message) return new Response("ok");

    const chat_id = String(message.chat.id);

    // Auth check
    if (env.ALLOWED_CHAT_ID && chat_id !== String(env.ALLOWED_CHAT_ID)) {
      return new Response("ok");
    }

    const text = message.text || message.caption || "";

    // ── /clear ───────────────────────────────────────────────────────────────
    if (text.trim() === "/clear") {
      await env.RAMP_KV.delete(`files:${chat_id}`);
      await sendMessage(env.BOT_TOKEN, chat_id, "Cleared. Staged files reset.");
      return new Response("ok");
    }

    // ── /status ──────────────────────────────────────────────────────────────
    if (text.trim() === "/status") {
      const staged = await getStaged(env, chat_id);
      if (staged.length === 0) {
        await sendMessage(env.BOT_TOKEN, chat_id, "No files staged. Send files then /ramp ASSET.");
      } else {
        const names = staged.map(f => `- ${f.file_name}`).join("\n");
        await sendMessage(env.BOT_TOKEN, chat_id, `Staged files (${staged.length}):\n${names}\n\nSend /ramp ASSET to generate report.`);
      }
      return new Response("ok");
    }

    // ── File upload — stage it ────────────────────────────────────────────────
    if (message.document) {
      const file_id   = message.document.file_id;
      const file_name = message.document.file_name || "upload";
      const caption   = (message.caption || "").replace(/\/ramp\s+\S+\s*/i, "").trim();

      const staged = await getStaged(env, chat_id);
      // Avoid duplicates by file_name
      const exists = staged.findIndex(f => f.file_name === file_name);
      if (exists >= 0) staged[exists] = { file_id, file_name, caption };
      else staged.push({ file_id, file_name, caption });

      await env.RAMP_KV.put(`files:${chat_id}`, JSON.stringify(staged), { expirationTtl: 86400 }); // 24h TTL

      await sendMessage(env.BOT_TOKEN, chat_id,
        `Staged: ${file_name}${caption ? `\nNote: ${caption}` : ""}\nTotal staged: ${staged.length} file(s)\n\nSend more files or /ramp ASSET to generate report.`
      );
      return new Response("ok");
    }

    // ── /ramp ASSET ──────────────────────────────────────────────────────────
    const match = text.match(/\/ramp\s+(\S+)/i);
    if (!match) return new Response("ok");

    const asset  = match[1].toUpperCase();
    const staged = await getStaged(env, chat_id);

    await sendMessage(env.BOT_TOKEN, chat_id,
      `Generating report for ${asset}...${staged.length > 0 ? `\nUsing ${staged.length} staged file(s).` : ""}\nThis takes 5-8 minutes.`
    );

    // Trigger GitHub Actions
    const resp = await fetch(
      `https://api.github.com/repos/${env.GITHUB_REPO}/dispatches`,
      {
        method: "POST",
        headers: {
          Authorization: `token ${env.GITHUB_TOKEN}`,
          Accept: "application/vnd.github.v3+json",
          "Content-Type": "application/json",
          "User-Agent": "ramp-bot-worker",
        },
        body: JSON.stringify({
          event_type: "ramp",
          client_payload: {
            asset,
            chat_id,
            staged_files: staged, // array of {file_id, file_name, caption}
          },
        }),
      }
    );

    if (!resp.ok) {
      const err = await resp.text();
      await sendMessage(env.BOT_TOKEN, chat_id, `Failed to start report: ${err}`);
    }

    // Clear staged files after dispatching
    await env.RAMP_KV.delete(`files:${chat_id}`);

    return new Response("ok");
  },
};


async function getStaged(env, chat_id) {
  const raw = await env.RAMP_KV.get(`files:${chat_id}`);
  return raw ? JSON.parse(raw) : [];
}

async function sendMessage(token, chat_id, text) {
  return fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id, text }),
  });
}
