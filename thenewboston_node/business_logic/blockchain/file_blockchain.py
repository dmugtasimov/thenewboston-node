import logging
import os.path
from typing import Generator

import msgpack

from thenewboston_node.business_logic.models.account_root_file import AccountRootFile
from thenewboston_node.business_logic.models.block import Block
from thenewboston_node.business_logic.storages.file_system import FileSystemStorage

from .base import BlockchainBase

logger = logging.getLogger(__name__)

# TODO(dmu) LOW: Move these constants to configuration files
ORDER_OF_ACCOUNT_ROOT_FILE = 10
ORDER_OF_BLOCK = 20
BLOCK_CHUNK_SIZE = 1000


class FileBlockchain(BlockchainBase):

    def __init__(self, base_directory, validate=True):
        if not os.path.isabs(base_directory):
            raise ValueError('base_directory must be an absolute path')

        self.account_root_files_directory = os.path.join(base_directory, 'account-root-files')
        self.blocks_directory = os.path.join(base_directory, 'blocks')
        self.base_directory = base_directory

        self.storage = FileSystemStorage()

        if validate:
            self.validate()

    # Account root files methods
    def persist_account_root_file(self, account_root_file: AccountRootFile):
        last_block_number = account_root_file.last_block_number

        prefix = ('.' if last_block_number is None else str(last_block_number)).zfill(ORDER_OF_ACCOUNT_ROOT_FILE)
        file_path = os.path.join(self.account_root_files_directory, f'{prefix}-arf.msgpack')
        self.storage.save(file_path, account_root_file.to_messagepack(), is_final=True)

    def iter_account_root_files(self) -> Generator[AccountRootFile, None, None]:
        storage = self.storage
        for file_path in storage.list_directory(self.account_root_files_directory):
            yield AccountRootFile.from_messagepack(storage.load(file_path))

    # Blocks methods
    def persist_block(self, block: Block):
        block_number = block.message.block_number
        chunk_number, offset = divmod(block_number, BLOCK_CHUNK_SIZE)

        chunk_block_number_start = chunk_number * BLOCK_CHUNK_SIZE
        start_str = str(chunk_block_number_start).zfill(ORDER_OF_BLOCK)

        chunk_block_number_end = chunk_block_number_start + BLOCK_CHUNK_SIZE - 1
        end_str = str(chunk_block_number_end).zfill(ORDER_OF_BLOCK)

        is_final = offset == BLOCK_CHUNK_SIZE - 1

        file_path = os.path.join(self.blocks_directory, f'{start_str}-{end_str}-block-chunk.msgpack')
        self.storage.append(file_path, block.to_messagepack(), is_final=is_final)

    def iter_blocks(self) -> Generator[Block, None, None]:
        storage = self.storage
        for file_path in storage.list_directory(self.blocks_directory):
            unpacker = msgpack.Unpacker()
            unpacker.feed(storage.load(file_path))
            for block_compact_dict in unpacker:
                yield Block.from_compact_dict(block_compact_dict)
