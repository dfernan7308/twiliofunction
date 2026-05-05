const { json, parseJsonBody } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { comparePassword, signToken, normalizeEmail } = require('./_lib/auth');

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return json(200, { ok: true });
  }

  if (event.httpMethod !== 'POST') {
    return json(405, { error: 'Method not allowed' });
  }

  try {
    const body = parseJsonBody(event);
    const email = normalizeEmail(body.email);
    const password = String(body.password || '').trim();

    if (!email || !password) {
      return json(400, { error: 'email and password are required' });
    }

    const supabase = getSupabaseAdmin();
    const { data: user, error } = await supabase
      .from('app_users')
      .select('id, username, email, role, phone, password_hash, is_active')
      .eq('email', email)
      .maybeSingle();

    if (error) {
      return json(500, { error: 'Failed to query user', details: error.message });
    }

    if (!user || !user.is_active) {
      return json(401, { error: 'Invalid credentials' });
    }

    const isValidPassword = await comparePassword(password, user.password_hash);
    if (!isValidPassword) {
      return json(401, { error: 'Invalid credentials' });
    }

    const token = signToken(user);

    return json(200, {
      token,
      user: {
        id: user.id,
        username: user.username,
        email: user.email,
        role: user.role,
        phone: user.phone
      }
    });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
