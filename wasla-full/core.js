// ================================================================
//  Wasla Shop - السيرفر لفيرسل (Vercel Serverless + Upstash Redis)
// ================================================================
//  دالة واحدة (api/[...path].js) بتتعامل مع كل الـ API، والبيانات في Upstash Redis:
//   wasla:db   = ملف البيانات (JSON)           wasla:ver = رقم النسخة (للمزامنة الآمنة)
//   img:<name> = صور الأجهزة                    s:/u:/k:  = جلسات الدخول
//   backup:<تاريخ> = نسخ احتياطية يومية (آخر 30 يوم)
//  كل كتابة بتتم بـ compare-and-set (Lua) فمفيش تحديثين بيمسحوا بعض حتى لو الدالة اتشغلت في مكانين.
//  الأوامر موفّرة: مزامنة الموقع = أمر واحد (GET ver) لما مفيش جديد، علشان الخطة المجانية (500 ألف أمر/شهر).
// ================================================================
const crypto = require('crypto');

const REST_URL = () => process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || '';
const REST_TOKEN = () => process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '';
const K_DB = 'wasla:db', K_VER = 'wasla:ver', LEGACY_KEY = 'wasla_shop_data';
const SESS_TTL = 30 * 86400, SESS_CACHE_MS = 120 * 1000;

// ---------------------------------------------------------------- Redis (REST)
async function rest(path, body) {
    const url = REST_URL(), token = REST_TOKEN();
    if (!url || !token) throw Object.assign(new Error('قاعدة البيانات غير مربوطة (متغيرات البيئة ناقصة)'), { status: 503 });
    const r = await fetch(url.replace(/\/$/, '') + path, { method: 'POST', headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const j = await r.json().catch(() => null);
    if (!r.ok) throw new Error('redis http ' + r.status + ' ' + (j && j.error || ''));
    return j;
}
async function cmd(args) {
    const j = await rest('', args);
    if (j.error) throw new Error(j.error);
    return j.result;
}
async function pipe(cmds) {
    const j = await rest('/pipeline', cmds);
    return j.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}
const CAS = "local cur = redis.call('GET', KEYS[1]) if cur == ARGV[1] or ((not cur) and ARGV[1] == '') then redis.call('SET', KEYS[2], ARGV[2]) redis.call('SET', KEYS[1], ARGV[3]) return 1 else return 0 end";

// ---------------------------------------------------------------- أدوات
const sha256 = s => crypto.createHash('sha256').update(s).digest('hex');
const str = (v, n) => String(v == null ? '' : v).slice(0, n);
function hashPassword(pw) {
    const salt = crypto.randomBytes(16);
    return 's1$' + salt.toString('base64') + '$' + crypto.scryptSync(String(pw), salt, 64).toString('base64');
}
function verifyPasswordAsync(pw, stored) {
    return new Promise(resolve => {
        try {
            const [v, s, h] = String(stored || '').split('$');
            if (v !== 's1' || !s || !h) return resolve(false);
            const expected = Buffer.from(h, 'base64');
            crypto.scrypt(String(pw), Buffer.from(s, 'base64'), 64, (err, key) => {
                if (err) return resolve(false);
                resolve(key.length === expected.length && crypto.timingSafeEqual(key, expected));
            });
        } catch (e) { resolve(false); }
    });
}
const normPhone = p => {
    let d = String(p || '').replace(/[^0-9]/g, '');
    if (d.startsWith('0020')) d = '0' + d.slice(4);
    else if (d.startsWith('20') && d.length === 12) d = '0' + d.slice(2);
    return d;
};
const nextVer = () => Date.now().toString(36) + crypto.randomBytes(3).toString('hex');
const SAFE_ID = /^[A-Za-z0-9_-]{3,40}$/;
const SAFE_USERNAME = /^[\p{L}\p{N}_. -]{2,40}$/u;
function cleanDeep(v, key) {
    if (typeof v === 'string') {
        if (key === 'password' || key === 'images') return v;
        return v.replace(/[<>]/g, c => (c === '<' ? '\uFF1C' : '\uFF1E'));
    }
    if (Array.isArray(v)) return v.map(x => cleanDeep(x, key));
    if (v && typeof v === 'object') { const o = {}; for (const [k, x] of Object.entries(v)) o[k] = cleanDeep(x, k); return o; }
    return v;
}

// ---------------------------------------------------------------- البيانات الافتراضية والترحيل
function defaultData() {
    const mk = (username, role, phone, email) => ({ username, passwordHash: hashPassword('123'), role, phone, email: email || '', mustChange: true });
    return {
        users: [mk('manager', 'manager', '01001234567'), mk('client', 'client', '01001234568', 'client@example.com'),
            mk('cashier', 'cashier', '01007654321', 'cashier@example.com'), mk('technician', 'technician', '01001234569')],
        techSeeded: true, requests: [], chats: [], logs: [], ratings: [], parts: [],
        settings: { companyPhone: '01001234567', emailNotifications: { onCreate: false, onProgress: false, onComplete: false }, notificationEmail: '', warrantyDays: 30, lateDays: 7, pickupDays: 7, loyaltyPointsPerOrder: 10 },
        _srv: { lastBackup: '', backups: [] }
    };
}
async function migrate(d) {
    d.users = Array.isArray(d.users) ? d.users : [];
    for (const u of d.users) {
        if (u.password !== undefined) {
            if (String(u.password) === '123') u.mustChange = true;
            u.passwordHash = hashPassword(u.password); delete u.password;
        }
        if (!u.passwordHash) { u.passwordHash = hashPassword(crypto.randomBytes(9).toString('hex')); u.mustChange = true; }
    }
    if (!d.techSeeded) {
        d.techSeeded = true;
        if (!d.users.some(u => u.role === 'technician') && !d.users.some(u => u.username === 'technician'))
            d.users.push({ username: 'technician', passwordHash: hashPassword('123'), role: 'technician', phone: '01001234569', email: '', mustChange: true });
    }
    for (const k of ['requests', 'chats', 'logs', 'ratings', 'parts']) if (!Array.isArray(d[k])) d[k] = [];
    delete d.activeSessions;
    d.settings = Object.assign({ companyPhone: '01001234567', notificationEmail: '', emailNotifications: { onCreate: false, onProgress: false, onComplete: false }, warrantyDays: 30, lateDays: 7, pickupDays: 7, loyaltyPointsPerOrder: 10 }, d.settings || {});
    for (const u of d.users) if (u.loyaltyPoints === undefined) u.loyaltyPoints = 0;
    const old = d._srv || {};
    d._srv = { lastBackup: old.lastBackup || '', backups: Array.isArray(old.backups) ? old.backups : [] };
    for (const r of d.requests) {
        if (r.status === 'cashier_received' && r.techStatus === 'received') r.status = 'manager_accepted';
        if (Array.isArray(r.images)) r.images = await extractImages(r.images);
    }
    return d;
}

// ---------------------------------------------------------------- الصور (كل صورة في مفتاح لوحدها)
const IMG_EXT = { 'image/jpeg': 'jpg', 'image/jpg': 'jpg', 'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif' };
const EXT_TYPE = { jpg: 'image/jpeg', png: 'image/png', webp: 'image/webp', gif: 'image/gif' };
const IMG_URL = /^\/api\/img\/([a-f0-9]{64}\.(jpg|png|webp|gif))$/;
async function extractImages(list) {
    const out = [];
    for (const img of list.slice(0, 6)) {
        if (typeof img !== 'string') continue;
        const m = /^data:(image\/[a-z+]+);base64,(.+)$/s.exec(img);
        if (m && IMG_EXT[m[1]]) {
            if (m[2].length > 1.4 * 1024 * 1024) continue;          // أكبر من ~1MB: اترفضت (الموقع بيضغط الصور قبل الرفع)
            const name = sha256(m[2]) + '.' + IMG_EXT[m[1]];
            await cmd(['SET', 'img:' + name, m[1] + '|' + m[2]]);
            out.push('/api/img/' + name);
        } else if (IMG_URL.test(img)) out.push(img);
        else { const lg = /^\/img\/([a-f0-9]{64}\.(jpg|png|webp|gif))$/.exec(img); if (lg) out.push('/api/img/' + lg[1]); }
    }
    return out;
}

// ---------------------------------------------------------------- الوصول للبيانات (مع كاش في الذاكرة + compare-and-set)
let cache = { ver: null, db: null };
const hideMgr = r => { const { techHandedBackAt, techHandedBackTs, ...rest } = r; return rest; };
const viewCache = new Map();
const sessCache = new Map();
let publicCache = { phone: '', exp: 0 };

async function initDb() {
    let legacy = null;
    try { const raw = await cmd(['GET', LEGACY_KEY]); legacy = typeof raw === 'string' ? JSON.parse(raw) : raw; } catch (e) {}
    const db = await migrate(legacy && Array.isArray(legacy.users) ? legacy : defaultData());
    const ver = nextVer();
    const ok = await cmd(['EVAL', CAS, 2, K_VER, K_DB, '', JSON.stringify(db), ver]);
    if (Number(ok) !== 1) return getDb();
    cache = { ver, db }; return cache;
}
async function getDb() {
    const ver = await cmd(['GET', K_VER]);
    if (!ver) return initDb();
    if (cache.db && cache.ver === ver) return cache;
    const [v2, raw] = await pipe([['GET', K_VER], ['GET', K_DB]]);
    cache = { ver: v2, db: JSON.parse(raw) };
    return cache;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function mutate(fn) {
    for (let i = 0; i < 10; i++) {
        if (i > 0) await sleep(15 + Math.random() * 60 * i);      // تأخير عشوائي بسيط لما يحصل تعارض
        const { db: base, ver } = await getDb();
        const work = JSON.parse(JSON.stringify(base));
        const out = await fn(work);
        const next = out || work;
        const newVer = nextVer();
        const ok = await cmd(['EVAL', CAS, 2, K_VER, K_DB, ver, JSON.stringify(next), newVer]);
        if (Number(ok) === 1) { cache = { ver: newVer, db: next }; viewCache.clear(); return next; }
        cache = { ver: null, db: null };
    }
    const e = new Error('تعارض في الحفظ، جرب تاني'); e.status = 409; throw e;
}

// ---------------------------------------------------------------- الجلسات
const uKey = username => 'u:' + sha256('u:' + username).slice(0, 32);
async function createSession(username) {
    const token = crypto.randomBytes(32).toString('hex'), h = sha256(token);
    const old = await cmd(['GET', uKey(username)]);
    const cmds = [['SET', 's:' + h, username, 'EX', SESS_TTL], ['SET', uKey(username), h, 'EX', SESS_TTL]];
    if (old && old !== h) { cmds.push(['DEL', 's:' + old], ['SET', 'k:' + old, '1', 'EX', 86400]); sessCache.delete(old); }
    await pipe(cmds);
    return token;
}
async function killSessions(username) {
    const old = await cmd(['GET', uKey(username)]);
    if (!old) return;
    sessCache.delete(old);
    await pipe([['DEL', 's:' + old], ['DEL', uKey(username)], ['SET', 'k:' + old, '1', 'EX', 86400]]);
}
async function authSession(req) {
    const m = /^Bearer (.+)$/.exec(req.headers.authorization || '');
    if (!m) throw Object.assign(new Error('سجّل الدخول من جديد'), { status: 401, code: 'invalid' });
    const h = sha256(m[1]);
    const c = sessCache.get(h);
    if (c && c.exp > Date.now()) return { username: c.u, hash: h };
    const u = await cmd(['GET', 's:' + h]);
    if (!u) {
        sessCache.delete(h);
        const k = await cmd(['GET', 'k:' + h]);
        throw Object.assign(new Error('سجّل الدخول من جديد'), { status: 401, code: k ? 'session_taken' : 'invalid' });
    }
    sessCache.set(h, { u, exp: Date.now() + SESS_CACHE_MS });
    if (sessCache.size > 500) sessCache.clear();
    return { username: u, hash: h };
}
async function rateLimited(key, max, windowSec) {
    const n = Number(await cmd(['INCR', 'rl:' + key]));
    if (n === 1) await cmd(['EXPIRE', 'rl:' + key, windowSec]);
    return n > max;
}

// ---------------------------------------------------------------- العرض حسب الدور
const pubUser = u => ({ username: u.username, role: u.role, phone: u.phone || '', email: u.email || '', mustChange: !!u.mustChange, loyaltyPoints: Number(u.loyaltyPoints) || 0 });
function viewFor(db, user, ver) {
    const key = ver + '|' + user.username;
    if (viewCache.has(key)) return viewCache.get(key);
    const me = user.username, role = user.role, s = db.settings, out = { me: pubUser(user) };
    if (role === 'manager') {
        out.users = db.users.map(pubUser); out.requests = db.requests; out.chats = db.chats; out.logs = db.logs;
        out.ratings = db.ratings; out.parts = db.parts; out.settings = s;
    } else if (role === 'cashier') {
        out.users = db.users.filter(u => u.role !== 'manager').map(pubUser); out.requests = db.requests.map(hideMgr);
        out.chats = db.chats.filter(c => c.sender === me || c.receiver === me); out.ratings = db.ratings;
        out.parts = db.parts.map(({ cost, ...p }) => p); out.settings = s;
    } else if (role === 'technician') {
        out.users = db.users.filter(u => u.role === 'cashier' || u.username === me).map(u => ({ ...pubUser(u), phone: u === user ? u.phone || '' : '' }));
        out.requests = db.requests.filter(r => r.sentToTech === me).map(r => { const { price, deposit, payments, techHandedBackAt, techHandedBackTs, ...rest } = r; return rest; });
        out.chats = db.chats.filter(c => c.sender === me || c.receiver === me);
        out.parts = db.parts.map(({ cost, ...p }) => p); out.settings = { companyPhone: s.companyPhone, warrantyDays: s.warrantyDays };
    } else {
        out.users = db.users.filter(u => u.role === 'cashier' || u.role === 'manager' || u.username === me)
            .map(u => ({ username: u.username, role: u.role, phone: u === user ? u.phone || '' : '', email: u === user ? u.email || '' : '', mustChange: u === user && !!u.mustChange, loyaltyPoints: u === user ? (Number(u.loyaltyPoints) || 0) : undefined }));
        out.requests = db.requests.filter(r => r.clientName === me).map(r => {
            const { internalNotes, techHandedBackAt, techHandedBackTs, ...rest } = r;
            if (rest.partsUsed) rest.partsUsed = rest.partsUsed.map(p => ({ name: p.name, qty: p.qty }));
            return rest;
        });
        out.chats = db.chats.filter(c => c.sender === me || c.receiver === me);
        out.ratings = db.ratings.filter(r => r.clientName === me);
        out.settings = { companyPhone: s.companyPhone, warrantyDays: s.warrantyDays, loyaltyPointsPerOrder: s.loyaltyPointsPerOrder };
    }
    const json = JSON.stringify(out);
    if (viewCache.size > 100) viewCache.clear();
    viewCache.set(key, json);
    return json;
}

// ---------------------------------------------------------------- تطبيق التغييرات (delta)
const APPEND_ARRAYS = ['history', 'payments', 'internalNotes', 'partsUsed'];
const TECH_FIELDS = ['progress', 'history', 'status', 'techStatus', 'techReceivedBy', 'techReceivedAt', 'techReceivedTs', 'techReceiptNo', 'acceptedByManagerAt', 'completedAt', 'completedTs', 'techHandedBackAt', 'techHandedBackTs', 'eta', 'quote', 'partsUsed', 'internalNotes', 'diagnosis'];
const CLIENT_FIELDS = ['status', 'pickedUpAt', 'pickedUpTs', 'pickedUpBy', 'quote'];
const CLIENT_CREATE = ['id', 'serialNumber', 'priority', 'deviceDetails', 'images', 'createdAt', 'createdAtTs', 'createdTs', 'clientEmail'];
const itemKey = x => (x && typeof x === 'object' && x.id) ? 'id:' + x.id : JSON.stringify(x);
const paidOf = r => (r.payments || []).reduce((a, p) => a + (Number(p.amount) || 0), 0);
function setField(target, k, v) {
    if (APPEND_ARRAYS.includes(k) && Array.isArray(v) && Array.isArray(target[k])) {
        const have = new Set(target[k].map(itemKey));
        for (const it of v) if (!have.has(itemKey(it))) target[k].push(it);
    } else target[k] = v;
}

async function applyOps(db, user, rawOps, rejected, ctx) {
    const ops = cleanDeep(rawOps);
    for (const coll of ['requests', 'chats', 'parts']) {
        const o = ops[coll];
        if (o && Array.isArray(o.upsert)) o.upsert = o.upsert.filter(x => x && SAFE_ID.test(String(x.id || '')));
    }
    if (ops.users && Array.isArray(ops.users.upsert)) ops.users.upsert = ops.users.upsert.filter(x => x && SAFE_USERNAME.test(String(x.username || '')));
    const me = user.username, role = user.role, isMgr = role === 'manager', isStaff = isMgr || role === 'cashier';
    const rej = w => rejected.push(w);

    const R = ops.requests || {};
    for (const patch of (R.upsert || [])) {
        if (!patch || !patch.id) continue;
        const ex = db.requests.find(r => r.id === patch.id);
        if (!ex) {
            if (role === 'client') {
                const rec = { status: 'pending', progress: 0, deviceType: '', receivedByCashierAt: '', acceptedByManagerAt: '', completedAt: '', history: [] };
                for (const k of CLIENT_CREATE) if (patch[k] !== undefined) rec[k] = patch[k];
                rec.clientName = me; rec.createdBy = me; rec.clientPhone = user.phone || '';
                rec.serialNumber = str(rec.serialNumber, 100); rec.deviceDetails = str(rec.deviceDetails, 1000);
                rec.images = await extractImages(Array.isArray(rec.images) ? rec.images : []);
                if (!rec.serialNumber) { rej('طلب بدون سيريال'); continue; }
                db.requests.push(rec);
            } else if (isStaff) {
                const rec = { ...patch }; rec.createdBy = me;
                rec.images = await extractImages(Array.isArray(rec.images) ? rec.images : []);
                db.requests.push(rec);
            } else rej('غير مسموح بإنشاء طلب');
            continue;
        }
        const { id, __del, ...fields } = patch;
        const prevStatus = ex.status;
        if (isStaff) {
            for (const [k, v] of Object.entries(fields)) { if (k === 'clientName' || k === 'createdBy') continue; setField(ex, k, v); }
            for (const k of (__del || [])) if (k !== 'id' && k !== 'clientName' && k !== 'createdBy') delete ex[k];
        } else if (role === 'technician') {
            if (ex.sentToTech !== me) { rej('جهاز غير مرسل إليك'); continue; }
            for (const [k, v] of Object.entries(fields)) {
                if (!TECH_FIELDS.includes(k)) continue;
                if (k === 'status' && !['manager_accepted', 'ready_for_pickup'].includes(v)) continue;
                setField(ex, k, v);
            }
        } else if (role === 'client') {
            if (ex.clientName !== me) { rej('طلب لا يخصك'); continue; }
            for (const [k, v] of Object.entries(fields)) {
                if (!CLIENT_FIELDS.includes(k)) continue;
                if (k === 'status') {
                    if (v === 'picked_up' && ex.status === 'ready_for_pickup' && (Number(ex.price) || 0) - paidOf(ex) <= 0) ex.status = 'picked_up';
                    else rej('لا يمكن تأكيد الاستلام (قد يوجد مبلغ متبقي)');
                } else if (k === 'quote') {
                    if (ex.quote && ex.quote.status === 'proposed' && v && ['approved', 'rejected'].includes(v.status))
                        ex.quote = { ...ex.quote, status: v.status, decidedAt: str(v.decidedAt, 40), decidedTs: Number(v.decidedTs) || Date.now() };
                } else ex[k] = v;
            }
        }
        if (prevStatus !== 'picked_up' && ex.status === 'picked_up') {
            const pts = Math.max(0, Number(db.settings.loyaltyPointsPerOrder) || 0);
            const cu = db.users.find(x => x.username === ex.clientName);
            if (cu && pts) { cu.loyaltyPoints = (Number(cu.loyaltyPoints) || 0) + pts; ex.loyaltyPointsAwarded = pts; }
        }
    }
    if (isMgr) db.requests = db.requests.filter(r => !(R.remove || []).includes(r.id));

    const C = ops.chats || {};
    for (const patch of (C.upsert || [])) {
        if (!patch || !patch.id) continue;
        const ex = db.chats.find(c => c.id === patch.id);
        if (!ex) {
            if (!patch.receiver || !patch.message) continue;
            db.chats.push({ id: str(patch.id, 40), sender: isMgr ? str(patch.sender || me, 40) : me, receiver: str(patch.receiver, 40), message: str(patch.message, 2000), timestamp: str(patch.timestamp, 60), read: false });
        } else if (patch.read !== undefined && (ex.receiver === me || isMgr)) ex.read = !!patch.read;
    }
    if (isMgr) db.chats = db.chats.filter(c => !(C.remove || []).includes(c.id));
    if (db.chats.length > 3000) db.chats = db.chats.slice(-3000);

    for (const rt of ((ops.ratings || {}).append || [])) {
        if (role !== 'client' && !isMgr) continue;
        const rq = db.requests.find(r => r.id === rt.requestId);
        if (!rq || (role === 'client' && rq.clientName !== me)) { rej('تقييم غير صالح'); continue; }
        db.ratings.push({ requestId: rt.requestId, clientName: rq.clientName, rating: Math.max(1, Math.min(5, Number(rt.rating) || 0)), comment: str(rt.comment, 500), timestamp: str(rt.timestamp, 60) });
    }

    const L = ops.logs || {};
    if (isMgr && L.clear) db.logs = [];
    for (const l of (L.append || [])) db.logs.push({ timestamp: str(l.timestamp, 60), user: isMgr ? str(l.user, 40) : me, action: str(l.action, 300) });
    if (db.logs.length > 1000) db.logs = db.logs.slice(-1000);

    const P = ops.parts || {};
    for (const patch of (P.upsert || [])) {
        if (!patch || !patch.id) continue;
        const ex = db.parts.find(p => p.id === patch.id);
        if (isMgr) { if (ex) { const { id, ...f } = patch; Object.assign(ex, f); } else db.parts.push({ ...patch }); }
        else if ((role === 'cashier' || role === 'technician') && ex && patch.qty !== undefined) ex.qty = Math.max(0, Number(patch.qty) || 0);
    }
    if (isMgr) db.parts = db.parts.filter(p => !(P.remove || []).includes(p.id));

    if (isMgr && ops.settings && ops.settings.set) Object.assign(db.settings, ops.settings.set);

    if (isMgr && ops.users) {
        for (const patch of (ops.users.upsert || [])) {
            if (!patch || !patch.username) continue;
            const ex = db.users.find(u => u.username === patch.username);
            const phone = patch.phone !== undefined ? str(patch.phone, 20).replace(/[^0-9]/g, '') : undefined;
            if (phone && db.users.some(u => u.phone === phone && u !== ex)) { rej(`الرقم ${phone} مستخدم`); continue; }
            if (!ex) {
                if (!patch.password || String(patch.password).length < 4) { rej('كلمة مرور قصيرة'); continue; }
                if (!['client', 'cashier', 'technician', 'manager'].includes(patch.role)) { rej('دور غير صالح'); continue; }
                db.users.push({ username: str(patch.username, 40), passwordHash: hashPassword(patch.password), role: patch.role, phone: phone || '', email: str(patch.email, 100), mustChange: true });
            } else {
                if (phone !== undefined) ex.phone = phone;
                if (patch.email !== undefined) ex.email = str(patch.email, 100);
                if (patch.loyaltyPoints !== undefined) ex.loyaltyPoints = Math.max(0, Number(patch.loyaltyPoints) || 0);
                if (patch.role && ex.username !== 'manager' && ['client', 'cashier', 'technician', 'manager'].includes(patch.role)) ex.role = patch.role;
                if (patch.password) {
                    if (String(patch.password).length < 4) { rej('كلمة مرور قصيرة'); continue; }
                    ex.passwordHash = hashPassword(patch.password); ex.mustChange = ex.username !== me;
                    if (ex.username !== me) ctx.kill.add(ex.username);
                }
            }
        }
        for (const name of (ops.users.remove || [])) {
            if (name === 'manager' || name === me) continue;
            db.users = db.users.filter(u => u.username !== name);
            ctx.kill.add(name);
        }
    }
}

// نسخة احتياطية يومية: بتتسجل جوه نفس الكتابة، والنسخة نفسها بتتحفظ بعد نجاح الكتابة
function planBackup(db, force) {
    const today = new Date().toISOString().slice(0, 10);
    if (!force && db._srv.lastBackup === today) return null;
    db._srv.lastBackup = today;
    const name = today + '.json';
    db._srv.backups = (db._srv.backups || []).filter(b => b.name !== name);
    db._srv.backups.push({ name, ts: Date.now(), size: JSON.stringify(db).length });
    db._srv.backups = db._srv.backups.slice(-30);
    return name;
}
async function saveBackup(db, name) {
    if (!name) return;
    try { await cmd(['SET', 'backup:' + name, JSON.stringify(db), 'EX', 32 * 86400]); } catch (e) { console.error('backup failed', e.message); }
}

// ---------------------------------------------------------------- الراوتر
const send = (res, status, obj) => { res.status(status).json(obj); };
const ipOf = req => String((req.headers['x-forwarded-for'] || req.socket && req.socket.remoteAddress || 'x')).split(',')[0].trim();
const fail = (status, error, code) => Object.assign(new Error(error), { status, code });

async function currentUser(req) {
    const s = await authSession(req);
    const { db } = await getDb();
    const user = db.users.find(u => u.username === s.username);
    if (!user) throw fail(401, 'الحساب غير موجود', 'invalid');
    return { user, session: s };
}
const needManager = u => { if (u.role !== 'manager') throw fail(403, 'للمدير فقط'); };

const routes = {
    'GET public': async (req, res) => {
        if (publicCache.exp < Date.now()) { const { db } = await getDb(); publicCache = { phone: db.settings.companyPhone, exp: Date.now() + 60000 }; }
        send(res, 200, { companyPhone: publicCache.phone });
    },

    'POST login': async (req, res) => {
        const body = req.body || {};
        const loginId = str(body.login, 60).trim(), password = str(body.password, 200);
        if (!loginId || !password) return send(res, 400, { error: 'أدخل اسم المستخدم وكلمة المرور' });
        if (await rateLimited(`login:${ipOf(req)}:${sha256(loginId).slice(0, 16)}`, 8, 600)) return send(res, 429, { error: 'محاولات كتير، استنى 10 دقايق وجرب تاني' });
        const { db } = await getDb();
        const u = db.users.find(x => x.username === loginId || (x.phone && x.phone === loginId));
        if (!u) return send(res, 401, { error: '❌ اسم المستخدم أو رقم الموبايل غير صحيح' });
        if (!(await verifyPasswordAsync(password, u.passwordHash))) return send(res, 401, { error: '❌ كلمة المرور غير صحيحة' });
        send(res, 200, { token: await createSession(u.username), user: pubUser(u) });
    },

    'POST logout': async (req, res) => {
        const s = await authSession(req);
        sessCache.delete(s.hash);
        await pipe([['DEL', 's:' + s.hash], ['DEL', uKey(s.username)]]);
        send(res, 200, { ok: true });
    },

    'POST register': async (req, res) => {
        if (await rateLimited(`reg:${ipOf(req)}`, 10, 3600)) return send(res, 429, { error: 'محاولات كتير، جرب بعد شوية' });
        const b = req.body || {};
        const username = str(b.username, 40).trim(), password = str(b.password, 200), phone = str(b.phone, 20).replace(/[^0-9]/g, '');
        if (!SAFE_USERNAME.test(username)) return send(res, 400, { error: '⚠️ اسم المستخدم لازم يكون حروف وأرقام ومسافة و - _ . بس (من حرفين لـ 40)' });
        if (password.length < 4) return send(res, 400, { error: '⚠️ كلمة المرور لازم 4 حروف على الأقل' });
        if (phone.length < 8) return send(res, 400, { error: '⚠️ رقم الموبايل غير صحيح' });
        let err = null;
        await mutate(async db => {
            err = null;
            if (db.users.some(u => u.username === username)) { err = [409, '❌ هذا الاسم موجود بالفعل']; return db; }
            if (db.users.some(u => u.phone === phone)) { err = [409, '❌ هذا الرقم موجود بالفعل']; return db; }
            db.users.push({ username, passwordHash: hashPassword(password), role: 'client', phone, email: str(b.email, 100).replace(/[<>]/g, ''), mustChange: false });
            return db;
        });
        if (err) return send(res, err[0], { error: err[1] });
        send(res, 200, { ok: true });
    },

    'POST password': async (req, res) => {
        const { user } = await currentUser(req);
        const { oldPassword, newPassword } = req.body || {};
        if (await rateLimited(`pw:${sha256(user.username).slice(0, 16)}`, 10, 600)) return send(res, 429, { error: 'محاولات كتير' });
        if (!(await verifyPasswordAsync(String(oldPassword || ''), user.passwordHash))) return send(res, 401, { error: '❌ كلمة المرور الحالية غير صحيحة' });
        if (String(newPassword || '').length < 4) return send(res, 400, { error: '⚠️ كلمة المرور الجديدة قصيرة (4 حروف على الأقل)' });
        if (String(newPassword) === '123') return send(res, 400, { error: '⚠️ اختار كلمة مرور غير الافتراضية' });
        const hash = hashPassword(newPassword);
        await mutate(async db => { const u = db.users.find(x => x.username === user.username); if (u) { u.passwordHash = hash; u.mustChange = false; } return db; });
        send(res, 200, { ok: true });
    },

    'GET data': async (req, res) => {
        const s = await authSession(req);
        const ver = await cmd(['GET', K_VER]);
        const etag = `"${ver}-${sha256(s.username).slice(0, 12)}"`;
        res.setHeader('Cache-Control', 'no-cache'); res.setHeader('ETag', etag);
        if (ver && req.headers['if-none-match'] === etag) return res.status(304).end();
        const { db, ver: v2 } = await getDb();
        const user = db.users.find(u => u.username === s.username);
        if (!user) throw fail(401, 'الحساب غير موجود', 'invalid');
        res.setHeader('ETag', `"${v2}-${sha256(s.username).slice(0, 12)}"`);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).send(viewFor(db, user, v2));
    },

    'POST sync': async (req, res) => {
        const { user } = await currentUser(req);
        const ops = (req.body && req.body.ops) || {};
        let rejected = [], ctx = null, backupName = null;
        const next = await mutate(async db => {
            rejected = []; ctx = { kill: new Set() };
            const u = db.users.find(x => x.username === user.username);
            if (!u) throw fail(401, 'الحساب غير موجود', 'invalid');
            await applyOps(db, u, ops, rejected, ctx);
            backupName = planBackup(db, false);
            return db;
        });
        await saveBackup(next, backupName);
        for (const name of ctx.kill) await killSessions(name).catch(() => {});
        send(res, 200, { ok: true, rejected });
    },

    'GET track': async (req, res) => {
        if (await rateLimited(`track:${ipOf(req)}`, 40, 600)) return send(res, 429, { error: 'محاولات كتير، جرب بعد شوية' });
        const { db } = await getDb();
        const id = str(req.query.id, 60).trim(), phone = normPhone(req.query.phone);
        const brief = r => ({ id: r.id, deviceType: r.deviceType || '', status: r.status, progress: r.progress || 0, createdAt: r.createdAt || '' });
        if (!id && phone.length >= 8) return send(res, 200, { list: db.requests.filter(r => normPhone(r.clientPhone) === phone).slice(-20).reverse().map(brief) });
        const r = db.requests.find(x => x.id === id);
        if (!r) return send(res, 404, { error: '❌ مفيش طلب بالرقم ده' });
        const t = { id: r.id, deviceType: r.deviceType || '', serialNumber: '***' + String(r.serialNumber || '').slice(-3), status: r.status, progress: r.progress || 0, priority: r.priority,
            createdAt: r.createdAt, receivedByCashierAt: r.receivedByCashierAt, techStatus: r.techStatus, techReceivedAt: r.techReceivedAt, completedAt: r.completedAt, pickedUpAt: r.pickedUpAt, eta: r.eta || '' };
        const wd = r.warrantyDays !== undefined && r.warrantyDays !== '' ? Number(r.warrantyDays) : Number(db.settings.warrantyDays || 0);
        t.warrantyDays = wd || 0; t.warrantyUntilTs = r.pickedUpTs && wd ? r.pickedUpTs + wd * 864e5 : 0;
        if (phone && normPhone(r.clientPhone) === phone) {
            t.verified = true; t.clientName = r.clientName; t.price = Number(r.price) || 0; t.paid = paidOf(r); t.remaining = Math.max(0, t.price - t.paid);
            t.quote = r.quote ? { amount: r.quote.amount, status: r.quote.status, note: r.quote.note } : null;
        }
        send(res, 200, { request: t, companyPhone: db.settings.companyPhone });
    },

    'GET img': async (req, res, parts) => {
        const name = parts[1] || '';
        const m = /^([a-f0-9]{64})\.(jpg|png|webp|gif)$/.exec(name);
        if (!m) return res.status(404).end();
        const raw = await cmd(['GET', 'img:' + name]);
        if (!raw) return res.status(404).end();
        const buf = Buffer.from(raw.slice(raw.indexOf('|') + 1), 'base64');
        res.setHeader('Content-Type', EXT_TYPE[m[2]]);
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        res.status(200).send(buf);
    },

    'GET backups': async (req, res) => {
        const { user } = await currentUser(req); needManager(user);
        const { db } = await getDb();
        send(res, 200, { list: [...(db._srv.backups || [])].reverse().map(b => ({ name: b.name, size: b.size || 0, updated: new Date(b.ts).toISOString() })) });
    },
    'POST backups/now': async (req, res) => {
        const { user } = await currentUser(req); needManager(user);
        let name = null;
        const next = await mutate(async db => { name = planBackup(db, true); return db; });
        await saveBackup(next, name);
        send(res, 200, { ok: true });
    },
    'POST restore': async (req, res) => {
        const { user } = await currentUser(req); needManager(user);
        const name = str(req.body && req.body.name, 40);
        if (!/^\d{4}-\d{2}-\d{2}\.json$/.test(name)) return send(res, 400, { error: 'اسم غير صالح' });
        const raw = await cmd(['GET', 'backup:' + name]);
        if (!raw) return send(res, 404, { error: 'النسخة غير موجودة (أو انتهت صلاحيتها)' });
        const restored = await migrate(JSON.parse(raw));
        await mutate(async db => { restored._srv = db._srv; return restored; });
        send(res, 200, { ok: true });
    },
    'GET export': async (req, res) => {
        const { user } = await currentUser(req); needManager(user);
        const { db } = await getDb();
        const copy = JSON.parse(JSON.stringify(db));
        copy._srv = { lastBackup: '', backups: [] };
        // الصور بتتضمن لحد ~3MB (حد فيرسل للرد)؛ الباقي بيفضل كروابط
        const names = []; copy.requests.forEach(r => (r.images || []).forEach(u => { const m = IMG_URL.exec(u); if (m) names.push(m[1]); }));
        let skipped = 0;
        if (names.length) {
            const vals = await cmd(['MGET', ...names.map(n => 'img:' + n)]);
            const map = new Map(); let budget = 3 * 1024 * 1024;
            names.forEach((n, i) => { const v = vals[i]; if (!v) return; const [type, b64] = [v.slice(0, v.indexOf('|')), v.slice(v.indexOf('|') + 1)]; if (budget - b64.length > 0) { budget -= b64.length; map.set(n, `data:${type};base64,${b64}`); } else skipped++; });
            copy.requests.forEach(r => { r.images = (r.images || []).map(u => { const m = IMG_URL.exec(u); return m && map.has(m[1]) ? map.get(m[1]) : u; }); });
        }
        res.setHeader('X-Images-Skipped', String(skipped));
        res.setHeader('Content-Disposition', `attachment; filename="wasla_shop_backup_${new Date().toISOString().slice(0, 10)}.json"`);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).send(JSON.stringify(copy));
    },
    'POST import': async (req, res) => {
        const { user } = await currentUser(req); needManager(user);
        const b = req.body;
        if (!b || !Array.isArray(b.users) || !b.users.some(u => u.role === 'manager')) return send(res, 400, { error: 'ملف غير صالح (لازم يحتوي على حساب مدير)' });
        const imported = await migrate(b);
        await mutate(async db => { imported._srv = db._srv; return imported; });
        send(res, 200, { ok: true });
    },
    'POST reset': async (req, res) => {
        const { user } = await currentUser(req); needManager(user);
        const fresh = await migrate(defaultData());
        await mutate(async db => { fresh._srv = db._srv; return fresh; });
        send(res, 200, { ok: true });
    }
};

async function handler(req, res) {
    try {
        let parts;
        if (req.query && req.query.path) parts = [].concat(req.query.path);
        else parts = new URL(req.url, 'http://x').pathname.replace(/^\/api\/?/, '').split('/').filter(Boolean);
        parts = parts.flatMap(p => String(p).split('/')).filter(Boolean);
        const key = req.method + ' ' + (parts[0] === 'backups' && parts[1] === 'now' ? 'backups/now' : parts[0] || '');
        const fn = routes[key];
        if (!fn) return send(res, 404, { error: 'غير موجود' });
        await fn(req, res, parts);
    } catch (e) {
        if (e && e.status) return send(res, e.status, { error: e.message, code: e.code });
        console.error(e);
        send(res, 500, { error: 'فشل في العملية' });
    }
}

module.exports = { handler, _internals: { cmd, cache: () => cache, resetCaches: () => { cache = { ver: null, db: null }; viewCache.clear(); sessCache.clear(); publicCache = { phone: '', exp: 0 }; } } };
