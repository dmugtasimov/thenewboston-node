import logging
import warnings
from copy import deepcopy
from operator import le, lt
from typing import Generator, Optional

from more_itertools import always_reversible, ilen

from thenewboston_node.business_logic.models import AccountState, BlockchainState

logger = logging.getLogger(__name__)


class BlockchainStateMixin:

    def persist_blockchain_state(self, account_root_file: BlockchainState):
        raise NotImplementedError('Must be implemented in a child class')

    def yield_blockchain_states(self) -> Generator[BlockchainState, None, None]:
        raise NotImplementedError('Must be implemented in a child class')

    def get_blockchain_states_count(self) -> int:
        # Highly recommended to override this method in the particular implementation of the blockchain for
        # performance reasons
        warnings.warn('Using low performance implementation of get_account_root_file_count() method (override it)')
        return ilen(self.yield_blockchain_states())

    def yield_blockchain_states_reversed(self) -> Generator[BlockchainState, None, None]:
        # Highly recommended to override this method in the particular implementation of the blockchain for
        # performance reasons
        warnings.warn(
            'Using low performance implementation of yield_blockchain_states_reversed() method (override it)'
        )
        yield from always_reversible(self.yield_blockchain_states())

    def add_blockchain_state(self, blockchain_state: BlockchainState):
        blockchain_state.validate(is_initial=blockchain_state.is_initial())
        self.persist_blockchain_state(blockchain_state)

    def get_first_blockchain_state(self) -> BlockchainState:
        # Override this method if a particular blockchain implementation can provide a high performance
        return next(self.yield_blockchain_states())

    def get_last_blockchain_state(self) -> BlockchainState:
        # Override this method if a particular blockchain implementation can provide a high performance
        return next(self.yield_blockchain_states_reversed())

    def has_blockchain_states(self):
        # Override this method if a particular blockchain implementation can provide a high performance
        try:
            self.get_first_blockchain_state()
        except StopIteration:
            return False

        return True

    def get_blockchain_state_by_block_number(self, block_number, inclusive: bool = False) -> BlockchainState:
        if block_number < -1:
            raise ValueError('next_block_number must be greater or equal to -1')

        op = le if inclusive else lt

        for blockchain_state in self.yield_blockchain_states_reversed():
            blockchain_state_block_number = blockchain_state.get_next_block_number() - 1
            if op(blockchain_state_block_number, block_number):
                return blockchain_state

        assert False

    def snapshot_blockchain_state(self):
        last_block = self.get_last_block()  # type: ignore
        if last_block is None:
            logger.warning('Blocks are not found: making account root file does not make sense')
            return None

        last_account_root_file = self.get_last_blockchain_state()  # type: ignore
        assert last_account_root_file is not None

        if not last_account_root_file.is_initial():
            assert last_account_root_file.last_block_number is not None
            if last_block.message.block_number <= last_account_root_file.last_block_number:
                logger.debug('The last block is already included in the last account root file')
                return None

        account_root_file = self.generate_blockchain_state()
        self.add_blockchain_state(account_root_file)

    def generate_blockchain_state(self, last_block_number: Optional[int] = None) -> BlockchainState:
        last_blockchain_state_snapshot = self.get_blockchain_state_by_block_number(last_block_number)
        assert last_blockchain_state_snapshot is not None
        logger.debug(
            'Generating blockchain state snapshot based on blockchain state with last_block_number=%s',
            last_blockchain_state_snapshot.last_block_number
        )

        blockchain_state = deepcopy(last_blockchain_state_snapshot)
        account_states = blockchain_state.account_states

        block = None
        for block in self.yield_blocks_from(blockchain_state.get_next_block_number()):  # type: ignore
            if last_block_number is not None and block.message.block_number > last_block_number:
                logger.debug('Traversed all blocks of interest')
                break

            logger.debug('Traversing block number %s', block.message.block_number)
            for account_number, block_account_state in block.message.updated_account_states.items():
                logger.debug('Found %s account state: %s', account_number, block_account_state)
                blockchain_state_account_state = account_states.get(account_number)
                if blockchain_state_account_state is None:
                    logger.debug('Account %s is met for the first time (empty lock is expected)', account_number)
                    assert block_account_state.balance_lock is None
                    blockchain_state_account_state = AccountState()
                    account_states[account_number] = blockchain_state_account_state

                for attribute in AccountState.get_field_names():  # type: ignore
                    value = getattr(block_account_state, attribute)
                    if value is not None:
                        setattr(blockchain_state_account_state, attribute, deepcopy(value))

        if block is not None:
            blockchain_state.last_block_number = block.message.block_number
            blockchain_state.last_block_identifier = block.message.block_identifier
            blockchain_state.last_block_timestamp = block.message.timestamp
            blockchain_state.next_block_identifier = block.hash

        return blockchain_state
