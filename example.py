import logging
import os
import shutil
import tempfile

import ray

from ray_tar_binary import try_files, copy_file, RayTaskClient, RayConfig, dispatch_binary_task, setup_logging

logger = logging.getLogger('example')


def demo():

    # The rust/c/cpp binary executable file's arguments and file's path
    number_1: int = 9
    number_2: int = 6
    exe_path = "examples/rust_demo/target/debug/rust_demo"

    temp_dir = tempfile.mkdtemp()
    logger.info(f"Temporary directory created at: {temp_dir}")

    rust_exe_path = try_files(os.curdir, [exe_path, exe_path + '.exe'])
    exec_path = os.path.join('bin', os.path.basename(rust_exe_path))

    copy_file(rust_exe_path, os.path.join(temp_dir, exec_path))

    # the rust/c/cpp binary executable file's arguments
    args_list = []
    # if your rust/c/cpp binary executable file has no arguments(e.g. examples/hello project),just comment out the following line
    args_list.append((str(number_1), str(number_2)))

    scheduler = RayTaskClient(RayConfig.default_client())

    tasks = dispatch_binary_task(scheduler, temp_dir, exec_path, args_list)
    scheduler.join(tasks)
    shutil.rmtree(temp_dir)


if __name__ == '__main__':
    setup_logging()
    ray.init()
    demo()
