# run demo
import hashlib
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path
from config import load

# hash file
def file_hash(path):
    result = hashlib.sha256()
    # open file bytes
    with path.open('rb') as stream:
        # hash file blocks
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()

# run demo peers
def demo():
    # locate demo folder
    source = Path(__file__).resolve().parent
    root = source / 'demo_run'
    # reset old output
    if root.is_symlink():
        raise ValueError('demo_run must be a regular directory')
    # remove old output
    if root.exists():
        shutil.rmtree(root)
    root.mkdir()
    # copy cfg files
    for name in ('Common.cfg', 'PeerInfo.cfg'):
        (root / name).write_bytes((source / name).read_bytes())
    # read demo cfg
    config, peers = load(root)
    # make demo bytes
    data = random.Random(2026).randbytes(config.size)
    # visit demo peers
    for info in peers:
        # set peer folder
        directory = root / f'peer_{info.id}'
        directory.mkdir()
        # use seed file
        if info.seed:
            (directory / config.filename).write_bytes(data)
    # save expected hash
    expected = hashlib.sha256(data).hexdigest()
    # track child processes
    processes, consoles = [], []
    # try operation
    try:
        # visit demo peers
        for info in peers:
            # open child console log
            output = (root / f'console_peer_{info.id}.txt').open('w')
            consoles.append(output)
            # build peer command
            command = [sys.executable, str(source / 'peerProcess.py'), str(info.id), str(root)]
            processes.append(subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT))
            time.sleep(0.25)
        # set demo deadline
        deadline = time.monotonic() + 180
        # wait for all peers
        while any(process.poll() is None for process in processes):
            # check peer failure
            if any(process.poll() not in (None, 0) for process in processes):
                raise RuntimeError('A peer failed; see console_peer_*.txt')
            # check demo timeout
            if time.monotonic() > deadline:
                raise TimeoutError('Demo timed out; see console files and peer logs')
            time.sleep(0.1)
        # check exit codes
        if any(process.returncode != 0 for process in processes):
            raise RuntimeError('A peer failed; see console_peer_*.txt')
        # visit demo peers
        for info in peers:
            # hash downloaded file
            actual = file_hash(root / f'peer_{info.id}' / config.filename)
            # check file hash
            if actual != expected:
                raise RuntimeError(f'File mismatch for peer {info.id}')
            print(f'Peer {info.id}: file hash matches')
        print('PASS: all peers finished, exited, and have identical files.')
    # clean up resources
    finally:
        # visit child processes
        for process in processes:
            # stop running child
            if process.poll() is None:
                process.terminate()
        # visit child processes
        for process in processes:
            # try operation
            try:
                process.wait(timeout=5)
            # kill stalled child
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        # close console files
        for output in consoles:
            output.close()

# start script
if __name__ == '__main__':
    # check args
    if len(sys.argv) != 1:
        sys.exit('Usage: python demo.py')
    # try operation
    try:
        demo()
    # handle conn or file error
    except (OSError, ValueError, RuntimeError, TimeoutError) as error:
        sys.exit(f'Error: {error}')
