import subprocess
from remork.router import bg, drain

procs = {}


def out_drain_handler(router, msg_id, data_type):
    def handler(data):
        if data:
            router.write_data(msg_id, data_type, data)
    return handler


def wait_proc(router, msg_id, proc, out_t, err_t):
    out_t.join()
    err_t.join()
    proc.wait()
    procs.pop(msg_id, None)
    router.done(msg_id, proc.returncode)


def run(router, msg_id, command, shell=True, cwd=None, env=None, has_stdin=False):
    p = subprocess.Popen(
        command, shell=shell, cwd=cwd, env=env,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    procs[msg_id] = p
    out_t = bg(drain, p.stdout, out_drain_handler(router, msg_id, 1))
    err_t = bg(drain, p.stderr, out_drain_handler(router, msg_id, 2))
    bg(wait_proc, router, msg_id, p, out_t, err_t)

    if has_stdin:  # pragma: no cover
        def handler(data_type, data):
            if data_type:
                if msg_id in procs:
                    procs[msg_id].stdin.write(data)
            else:
                if msg_id in procs:
                    procs[msg_id].stdin.close()

        router.data_subscribe(msg_id, handler)


#==LOCAL==
def on_result(result, value):
    return (value, b''.join(result.data.get(1, [])), b''.join(result.data.get(2, [])))


def run_helper(router, command, env=None, cwd=None, shell=True):
    return router.call('remork.process', 'run', command, env=env, cwd=cwd, shell=shell, on_result_=on_result)
