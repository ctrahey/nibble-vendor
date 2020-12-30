import asyncio, aiomysql, os
from contextlib import asynccontextmanager
from hypercorn.middleware import DispatcherMiddleware
from pprint import pprint as pp
from prometheus_client import start_http_server, Counter, make_asgi_app
from quart import Quart, request
import base64

app = Quart(__name__)
app_dispatch = DispatcherMiddleware({
    "/metrics": make_asgi_app(),
    "/": app
})

c = Counter('requests', 'Requests')


@asynccontextmanager
async def get_connection():
    conn = await aiomysql.connect(
        host=os.environ.get('DB_HOST', 'mysql-history.data-services.svc.cluster.local'),
        port=os.environ.get('DB_PORT', 3306),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        db=os.environ.get('DB_DATABASE', 'history'))
    try:
        yield conn
    finally:
        conn.close()


@app.route('/job/lease/<job_key>')
async def lease(job_key):
    job_key = base64.b64decode(job_key)
    print(f"Looking up job for key {job_key}", flush=True)

    ## First, we check if we need to initialize this job
    select_sql = "SELECT id FROM nibbles WHERE job_key=%s LIMIT 1"
    async with get_connection() as conn:
        conn.begin()
        async with conn.cursor() as cur:
            await cur.execute(select_sql, (job_key))
            r = await cur.fetchall()
            if 0 == len(r):
                insert_sql = "INSERT INTO nibbles (job_key, phase, token, status) VALUES (%s, 0, '/init/nibbles', 'AVAILABLE')"
                await cur.execute(insert_sql, job_key)
                await conn.commit()
    select_sql = "SELECT id, phase, token FROM nibbles WHERE job_key=%s AND status='AVAILABLE' ORDER BY phase ASC, id ASC LIMIT 1 FOR UPDATE"
    async with get_connection() as conn:
        conn.begin()
        async with conn.cursor() as cur:
            await cur.execute(select_sql, (job_key))
            r = await cur.fetchall()
            if 0 == len(r):
                return {'status':'No jobs available'}, 410
            nibble_id, phase, token = r[0]
            # check if we are good to start this phase
            phase_sql = "SELECT 1 FROM nibbles WHERE job_key=%s AND status!='COMPLETE' AND phase < %s"
            await cur.execute(phase_sql, (job_key, phase))
            r = await cur.fetchall()
            if 0 != len(r):
                return {'status': 'Awaiting completion of prior phase'}, 412
            await cur.execute("UPDATE nibbles SET status='LEASED' WHERE id=%s", nibble_id)
            await conn.commit()
    return {'token': token, 'nibble_id': nibble_id}, 201


@app.route('/nibble/<nibble_id>/complete')
async def complete(nibble_id):
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE nibbles SET status='COMPLETE' WHERE id=%s ", nibble_id)
            await conn.commit()
    return {'status': "ok"}


@app.route('/nibble/<nibble_id>/error')
async def error(nibble_id):
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE nibbles SET status='ERROR' WHERE id=%s", nibble_id)
            await conn.commit()
    return {'status': "ok"}


@app.route('/nibbles/<job_key>/init', methods=['POST'])
async def nibbles_init(job_key):
    input = await request.get_json()
    input_text = await request.get_data()
    pp(input)
    pp
    job_key = base64.b64decode(job_key)
    data = input['nibbles']
    data = [(job_key, t['phase'], t['slug']) for t in data]
    print(f"Inserting {len(data)} nibbles")
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO nibbles (job_key, phase, token)"
                "values (%s,%s,%s)", data)
            await conn.commit()
