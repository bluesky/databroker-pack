import collections
import hashlib
from pathlib import Path
import subprocess

from suitcase.utils import SuitcaseUtilsTypeError, SuitcaseUtilsValueError, ModeError


class SSHManager:
    """
    Write to files on a remote machine via ssh <host> 'cat <path>'

    This is really gross and the author absolves knowlege of it.
    """

    def __init__(self, host, directory):
        self._reserved_names = set()
        self._artifacts = collections.defaultdict(list)
        self._host = host
        self._directory = Path(directory)
        self.buffers = {}  # maps postfixes to buffer objects

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    @property
    def artifacts(self):
        return dict(self._artifacts)

    def reserve_name(self, label, postfix):
        """
        This action is not valid on this manager. It will always raise.
        Parameters
        ----------
        label : string
            A label for the sort of content being stored, such as
            'stream_data' or 'metadata'.
        postfix : string
            Relative file path. Must be unique for this Manager.
        Raises
        ------
        SuitcaseUtilsTypeError
        """
        raise SuitcaseUtilsTypeError(
            f"{type(self)} is incompatible with exporters that require "
            f"explicit filenames."
        )

    def open(self, label, postfix, mode, encoding=None, errors=None):
        """
        Request a file handle.

        Like the built-in open function, this may be used as a context manager.

        Parameters
        ----------
        label : string
            A label for the sort of content being stored, such as
            'stream_data' or 'metadata'.
        postfix : string
            Relative file path (simply used as an identifer in this case, as
            there is no actual file). Must be unique for this Manager.
        mode : {'x', 'xt', xb'}
            'x' or 'xt' for text, 'xb' for binary
        encoding : string or None
            Not used. Accepted for compatibility with built-in open().
        errors : string or None
            Not used. Accepted for compatibility with built-in open().
        Returns
        -------
        file : handle
        """
        # Of course, in-memory buffers have no filepath, but we still expect
        # postfix to be a thing that looks like a relative filepath, and we use
        # it as a unique identifier for a given buffer.
        if Path(postfix).is_absolute():
            raise SuitcaseUtilsValueError(
                f"The postfix {postfix} must be structured like a relative "
                f"file path."
            )
        name = Path(postfix).expanduser()
        if name in self._reserved_names:
            raise SuitcaseUtilsValueError(
                f"The postfix {postfix!r} has already been used."
            )
        self._reserved_names.add(name)
        if mode in ("x", "xt"):
            buffer = PipeStringToCat(self._host, str(self._directory / name))
        elif mode == "xb":
            buffer = PipeBytesToCat(self._host, str(self._directory / name))
        else:
            raise ModeError(
                f"The mode passed to MemoryBuffersManager.open is {mode} but "
                f"needs to be one of 'x', 'xt' or 'xb'."
            )
        self._artifacts[label].append(buffer)
        self.buffers[postfix] = buffer
        return buffer

    def close(self):
        """Close all buffers opened by the manager.
        """
        for f in self.buffers.values():
            f.close()


class PipeToCat:
    "A base class for piping to ssh <host> 'cat <path>'"

    def __init__(self, host, path):
        self._host = host
        self._path = path
        # Ensure directory exists.
        subprocess.run(
            ["ssh", host, "mkdir", "-p", str(Path(path).parent)], capture_output=True
        )
        self._process = subprocess.Popen(
            ["ssh", host, f"cat > {path}"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self._host}, {self._path})"

    @property
    def path(self):
        return self._path

    def close(self):
        self._process.stdin.close()
        self._process.wait()

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()


class PipeStringToCat(PipeToCat):
    def write(self, s):
        return self._process.stdin.write(s.encode())


class PipeBytesToCat(PipeToCat):
    def write(self, b):
        return self._process.stdin.write(b)


def root_hash(salt, root):
    """
    Generate a deterministic hash for a given salt and root.

    Parameters
    ----------
    salt: bytes
    root: Union[string, Path]

    Examples
    --------

    Generate a salt and hash a root (string or Path).

    >>> import secrets
    >>> salt = secrets.token_hex(32).encode()
    >>> root_hash(salt, root)
    """
    return hashlib.md5(str(root).encode() + salt).hexdigest()


class CatalogNameExists(ValueError):
    ...
