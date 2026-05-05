const { json } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { requireAuth } = require('./_lib/auth');

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return json(200, { ok: true });
  }

  if (event.httpMethod !== 'GET') {
    return json(405, { error: 'Method not allowed' });
  }

  const auth = requireAuth(event);
  if (!auth.ok) {
    return json(auth.statusCode, auth.body);
  }

  try {
    const supabase = getSupabaseAdmin();
    const { data, error } = await supabase
      .from('incidents')
      .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, created_at')
      .order('created_at', { ascending: false })
      .limit(200);

    if (error) {
      return json(500, { error: 'Failed to fetch incidents', details: error.message });
    }

    return json(200, { incidents: data || [] });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
