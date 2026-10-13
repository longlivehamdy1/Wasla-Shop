// دالة واحدة لكل الـ API (علشان خطة فيرسل المجانية بتسمح بـ 12 دالة بس)
const { handler } = require('../lib/core.js');
module.exports = handler;
module.exports.config = { maxDuration: 30 };
