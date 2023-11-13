"""
VFS for Github
"""

import asyncio
import dataclasses
import os

from typing import List, Optional

from shenaniganfs.fs import (
    FileType,
    FSENTRY,
    SimpleFS,
    SimpleDirectory,
    SimpleFSEntry,
    VerifyingFileHandleEncoder,
)
from shenaniganfs.fs_manager import EvictingFileSystemManager, create_fs
from shenaniganfs.nfs_utils import serve_nfs


@dataclasses.dataclass
class Org(SimpleFSEntry):
    type: FileType = dataclasses.field(default=FileType.DIR, init=False)
    name: str = dataclasses.field()

    @property
    def child_ids(self) -> List[int]:
        # TODO: Create Project entries for projects under self.name
        return []


class GithubFS(SimpleFS):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_only = True
        self.num_blocks = 1
        self.free_blocks = 0
        self.avail_blocks = 0

        self.track_entry(SimpleDirectory(
            mode=0o0777,
            name=b"",
            root_dir=True,
        ))

    def lookup(self, directory: FSENTRY, name: bytes) -> Optional[FSENTRY]:
        return super().lookup(directory, name) or self._lookup(directory, name)

    def _lookup(self, directory: FSENTRY, name: bytes) -> Optional[FSENTRY]:
        attrs = dict(
            mode=0o0777,
        )
        self._verify_size_quota(len(name) * 2)
        return self._base_create(directory, name, attrs, Org)


async def main():
    fs_manager = EvictingFileSystemManager(
        VerifyingFileHandleEncoder(os.urandom(32)),
        factories={
            b"/github": lambda call_ctx: create_fs(GithubFS, call_ctx),
        },
    )
    await serve_nfs(fs_manager, use_internal_rpcbind=True)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
