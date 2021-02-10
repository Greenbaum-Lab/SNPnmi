import asyncio

async def run(cmd):
    proc = await asyncio.create_subprocess_shell(cmd,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    print(f'[{cmd!r} exited with {proc.returncode}]')
    if stdout:
        print(f'[stdout]\n{stdout.decode()}')
    if stderr:
        print(f'[stderr]\n{stderr.decode()}')

def generate_cmd(min_window_index, max_window_index):
    mac_maf = 'maf'
    class_name = '0.49'
    min_minor_freq_expected = 0.49
    max_minor_freq_expected = 0.5
    min_minor_count_expected = -1
    max_minor_count_expected = -1
    classes_folder = r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/'
    cmd = f'python3 ./calc_distances_in_window.py {mac_maf} {class_name} {min_window_index} {max_window_index} {min_minor_freq_expected} {max_minor_freq_expected} {min_minor_count_expected} {max_minor_count_expected} {classes_folder}'
    print(cmd)
    return cmd

async def main():
    import time
    s = time.time()
    await asyncio.gather(
        run(generate_cmd(0,1)),
        run(generate_cmd(1,2)),
        run(generate_cmd(2,3)),
        run(generate_cmd(3,4)))
    print(f'{(time.time()-s)/60} minutes')

asyncio.run(main())
