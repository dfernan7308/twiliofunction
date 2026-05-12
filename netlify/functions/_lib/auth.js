const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');

const normalizePhone = (value) => String(value || '').replace(/[^\d]/g, '');
const normalizeEmail = (value) => String(value || '').trim().toLowerCase();

const getJwtSecret = () => {
  if (!process.env.JWT_SECRET) {
    throw new Error('Missing JWT_SECRET environment variable.');
  }

  return process.env.JWT_SECRET;
};

const signToken = (user) => {
  const tokenPayload = {
    sub: user.id,
    username: user.username,
    email: user.email,
    role: user.role,
    phone: user.phone || null,
    area_id: user.area_id || null,
    area_code: user.area && user.area.code ? user.area.code : null,
    area_name: user.area && user.area.name ? user.area.name : null
  };

  return jwt.sign(tokenPayload, getJwtSecret(), {
    expiresIn: '12h'
  });
};

const verifyToken = (token) => jwt.verify(token, getJwtSecret());

const getTokenFromEvent = (event) => {
  const authorization = event && event.headers
    ? event.headers.authorization || event.headers.Authorization
    : '';

  if (!authorization) {
    return '';
  }

  const [scheme, token] = authorization.split(' ');
  if (!scheme || !token || scheme.toLowerCase() !== 'bearer') {
    return '';
  }

  return token.trim();
};

const requireAuth = (event, options = {}) => {
  const token = getTokenFromEvent(event);
  if (!token) {
    return {
      ok: false,
      statusCode: 401,
      body: { error: 'Unauthorized' }
    };
  }

  try {
    const decoded = verifyToken(token);
    if (options.adminOnly && decoded.role !== 'admin') {
      return {
        ok: false,
        statusCode: 403,
        body: { error: 'Admin role required' }
      };
    }

    return {
      ok: true,
      user: decoded
    };
  } catch (error) {
    return {
      ok: false,
      statusCode: 401,
      body: { error: 'Invalid token' }
    };
  }
};

const hashPassword = async (password) => bcrypt.hash(password, 10);
const comparePassword = async (plainText, hash) => bcrypt.compare(plainText, hash);

module.exports = {
  normalizePhone,
  normalizeEmail,
  signToken,
  requireAuth,
  hashPassword,
  comparePassword
};
