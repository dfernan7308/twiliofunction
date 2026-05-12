const { json, parseJsonBody } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { requireAuth } = require('./_lib/auth');

const normalizeAreaCode = (value) => String(value || '')
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .toUpperCase()
  .replace(/[^A-Z0-9]+/g, '_')
  .replace(/^_+|_+$/g, '');

const parseTags = (value) => {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || '').trim()).filter(Boolean);
  }

  return String(value || '')
    .split(/[\n,]+/)
    .map((item) => item.trim())
    .filter(Boolean);
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return json(200, { ok: true });
  }

  if (event.httpMethod !== 'POST') {
    return json(405, { error: 'Method not allowed' });
  }

  const auth = requireAuth(event, { adminOnly: true });
  if (!auth.ok) {
    return json(auth.statusCode, auth.body);
  }

  try {
    const body = parseJsonBody(event);
    const name = String(body.name || '').trim();
    const code = normalizeAreaCode(body.code || name);
    const tags = parseTags(body.tags);
    const isActive = typeof body.is_active === 'boolean' ? body.is_active : true;

    if (!name) {
      return json(400, { error: 'name is required' });
    }

    if (!code) {
      return json(400, { error: 'code is required' });
    }

    const supabase = getSupabaseAdmin();
    const { data, error } = await supabase
      .from('areas')
      .insert({
        code,
        name,
        tags,
        is_active: isActive
      })
      .select('id, code, name, tags, is_active, created_at')
      .single();

    if (error) {
      const status = error.code === '23505' ? 409 : 500;
      return json(status, { error: 'Failed to create area', details: error.message });
    }

    return json(201, { area: data });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};