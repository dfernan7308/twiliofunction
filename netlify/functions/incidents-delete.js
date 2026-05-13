const { json, parseJsonBody } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { requireAuth } = require('./_lib/auth');

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
    const id = String(body.id || '').trim();
    const problemId = String(body.problem_id || body.problemId || '').trim().toUpperCase();

    if ((!id || !/^-?\d+$/.test(id)) && !problemId) {
      return json(400, { error: 'id or problem_id is required' });
    }

    const supabase = getSupabaseAdmin();
    let deletedRows = [];

    if (id && /^-?\d+$/.test(id)) {
      const { data: deletedById, error: deleteByIdError } = await supabase
        .from('incidents')
        .delete()
        .eq('id', id)
        .select('id, problem_id');

      if (deleteByIdError) {
        return json(500, { error: 'Failed to delete incident by id', details: deleteByIdError.message });
      }

      deletedRows = Array.isArray(deletedById) ? deletedById : [];
    }

    if ((!deletedRows.length) && problemId) {
      const { data: deletedByProblem, error: deleteByProblemError } = await supabase
        .from('incidents')
        .delete()
        .eq('problem_id', problemId)
        .select('id, problem_id');

      if (deleteByProblemError) {
        return json(500, { error: 'Failed to delete incident by problem_id', details: deleteByProblemError.message });
      }

      deletedRows = Array.isArray(deletedByProblem) ? deletedByProblem : [];
    }

    if (!Array.isArray(deletedRows) || !deletedRows.length) {
      return json(404, { error: 'Incident not found' });
    }

    return json(200, {
      ok: true,
      deletedCount: deletedRows.length,
      deletedId: String(deletedRows[0].id),
      deletedProblemId: String(deletedRows[0].problem_id || problemId || '')
    });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
