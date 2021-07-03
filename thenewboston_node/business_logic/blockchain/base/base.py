from typing import Optional

from thenewboston_node.business_logic.models import BlockchainState, Node
from thenewboston_node.core.utils.types import hexstr


class BaseMixin:

    def yield_blocks_till_snapshot(self, from_block_number: Optional[int] = None):
        """Return generator of blocks traversing from `from_block_number` block (or head block if not specified)
        to the block of the closest blockchain state (exclusive: the blockchain state block is not
        traversed).
        """
        raise NotImplementedError('Must be implemented in a child class')

    def yield_blocks_slice(self, from_block_number: int, to_block_number: int):
        raise NotImplementedError('Must be implemented in a child class')

    def yield_account_states(self, block_number):
        raise NotImplementedError('Must be implemented in a child class')

    def get_account_state_attribute_value(self, account: hexstr, attribute: str, on_block_number: int):
        raise NotImplementedError('Must be implemented in a child class')

    def get_node_by_identifier(self, identifier: hexstr, on_block_number: Optional[int] = None) -> Optional[Node]:
        """Return node declared with `identifier` known after `on_block_number` is applied.

        Args:
            identifier (hexstr): identifier of the node (an account that has declared the node)
            on_block_number (int): inclusive block number where the block declaration is retrieved from
                Special values:
                    - `-1`: no blocks are included (the node is returned from blockchain genesis state)
                    - `None`: latest known block is included

        Returns:
            Node instance or `None` if node is not considered declared on the requested block number
        """
        raise NotImplementedError('Must be implemented in a child class')

    def get_current_block_number(self) -> int:
        raise NotImplementedError('Must be implemented in a child class')

    def get_first_blockchain_state(self) -> BlockchainState:
        raise NotImplementedError('Must be implemented in a child class')

    def get_blockchain_state_by_block_number(self, block_number, inclusive: bool = False) -> BlockchainState:
        raise NotImplementedError('Must be implemented in a child class')
