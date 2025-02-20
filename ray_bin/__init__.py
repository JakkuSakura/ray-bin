import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from io import BytesIO
from typing import List, Any, Sequence, BinaryIO, Optional

import ray
from ray import ObjectRef

# ============================================================
# Logger settings
# ============================================================

logger = logging.getLogger('ray-bin')


# ============================================================
# File and tar function
# ============================================================

def try_files(base_dir: str, files: List[str]) -> Optional[str]:
    """
    Try to find the first file that exists in the list of files.
    :param base_dir: the base directory
    :param files: the list of files
    :return: the first file that exists
    """
    for file in files:
        if os.path.exists(os.path.join(base_dir, file)):
            return file
    return None


def copy_file(src_file: str, dest_file: str):
    """
    Copy a file from source to destination.
    :param src_file: the source file
    :param dest_file: the destination file
    :return:
    """
    # mkdirs
    os.makedirs(os.path.dirname(dest_file), exist_ok=True)
    shutil.copy(src_file, dest_file)


def temp_tarball_file(name: str) -> tempfile._TemporaryFileWrapper:
    """
    Create a tarball name from the package name. with a rantom number
    :param name:
    :return:
    """
    return tempfile.NamedTemporaryFile(prefix=name, suffix='.tar.gz')


def create_tarball(output_file: BinaryIO, workdir: str, excludes: list = None):
    """Create a .tar.gz archive of the source directory, excluding specified files."""
    if excludes is None:
        excludes = []
    logger.info(f"Creating tarball from %s, excluding %s", workdir, excludes)
    try:
        with tarfile.open(fileobj=output_file, mode="w:gz") as tar:
            for item in os.listdir(workdir):
                if item in excludes:
                    continue
                tar.add(os.path.join(workdir, item), arcname=item)
        logger.info(f"Tarball for %s created successfully.", workdir)
    except Exception as e:
        logger.error(f"Failed to create tarball: {e}")
        raise


def create_temp_tarball(workdir: str, excludes: list = None) -> BinaryIO:
    temp_file = temp_tarball_file(os.path.basename(workdir))
    create_tarball(temp_file, workdir, excludes)
    temp_file.seek(0)
    return temp_file


def extract_tarball(tarball: BinaryIO | bytes, dest: str):
    """Extract a tarball to a specified destination."""
    if isinstance(tarball, bytes):
        tarball = BytesIO(tarball)

    logger.info(f"Extracting tarball at: {dest}")

    with tarfile.open(fileobj=tarball, mode='r:gz') as tar:
        tar.extractall(dest)

    logger.info(f"Tarball extracted to: {dest}")


def extract_temp_tarball(tarball: BinaryIO | bytes, prefix: str = None) -> str:
    extract_dir = tempfile.mkdtemp(prefix=prefix)
    print(f"Temporary directory created at: {extract_dir}")
    extract_tarball(tarball, extract_dir)
    return extract_dir


# ============================================================
# Ray task packaging class and method
# ============================================================

DEFAULT_HOST = "127.0.0.1"
DEFAULT_LISTEN = "0.0.0.0"
DEFAULT_PORT = 6379


@dataclass
class RayConfig:
    host: str
    port: int

    @classmethod
    def default_client(cls):
        return cls(host=DEFAULT_HOST, port=DEFAULT_PORT)

    @classmethod
    def default_server(cls):
        return cls(host=DEFAULT_HOST, port=DEFAULT_PORT)

    def address(self):
        return f"{self.host}:{self.port}"


class RayTaskClient:
    def __init__(self, scheduler: RayConfig):
        self.scheduler = scheduler
        if not ray.is_initialized():
            ray.init(address=scheduler.address())

    def check(self):
        return ray.is_initialized()

    def stop(self):
        ray.shutdown()

    def ensure_remote(self, func):
        if isinstance(func, ray.remote_function.RemoteFunction):
            return func
        return ray.remote(func)

    def submit(self, func, *args, **kwargs) -> ObjectRef:
        remote_fn = self.ensure_remote(func)
        remote_ref = remote_fn.remote(*args, **kwargs)
        return remote_ref

    def dispatch(self, func, args: List[Sequence[Any]] = None, kwargs: List[dict] = None) -> List[ObjectRef]:
        assert args is not None or kwargs is not None, 'args and kwargs should not be both None'
        if args is None:
            args = [[] for _ in range(len(kwargs))]
        if kwargs is None:
            kwargs = [{} for _ in range(len(args))]
        assert len(args) == len(kwargs), 'args and kwargs should have the same length'
        remote_fn = self.ensure_remote(func)
        remote_refs = [remote_fn.remote(*arg, **kwarg) for arg, kwarg in zip(args, kwargs)]
        return remote_refs

    def join(self, tasks: List[ObjectRef] | ObjectRef):
        if isinstance(tasks, ObjectRef):
            return ray.get(tasks)
        return ray.get(tasks)

    def get(self, object_ref):
        return ray.get(object_ref)


# ============================================================
# The logic of uploading the tar to ray's storage, and decompress
# the tar and execute the program in '_decompress_and_run_executable'
# ============================================================

@ray.remote
def _decompress_and_run_executable(tarball: bytes, executable: str, args: Sequence[str]) -> None:
    """
    Decompress the tarball and run the executable with the arguments.
    :param tarball: the tarball
    :param executable: the executable
    :param args: the CLI arguments
    :return:
    """
    extract_dir = extract_temp_tarball(tarball)
    full_args = [os.path.join(extract_dir, executable)]
    full_args.extend(args)
    subprocess.check_call(full_args, shell=True, cwd=extract_dir)


def dispatch_binary_task(scheduler: RayTaskClient, workdir: str, executable: str, args: List[Sequence[str]]):
    """
    Dispatch a binary task to the remote scheduler.
    :param executable: the executable to run
    :param workdir: the working directory of the binary
    :param args: the arguments of the binary
    :return:
    """
    tarball = create_temp_tarball(workdir)
    tarball_object_ref = ray.put(tarball.read())
    logger.info("the arguments of the binary:%s", args)

    tasks = scheduler.dispatch(_decompress_and_run_executable,
                               kwargs=[
                                   {
                                       "tarball": tarball_object_ref,
                                       "executable": executable,
                                       "args": args1,
                                   } for args1 in (args or [[]])]
                               )
    return tasks
