import pytest

from thenewboston_node.business_logic.blockchain.base import BlockchainBase
from thenewboston_node.business_logic.models.block import Block


@pytest.mark.usefixtures('forced_mock_network', 'get_primary_validator_mock', 'get_preferred_node_mock')
def test_can_make_blockchain_state_on_last_block(
    forced_memory_blockchain: BlockchainBase, blockchain_genesis_state, treasury_account_key_pair,
    user_account_key_pair, primary_validator, preferred_node
):
    blockchain = forced_memory_blockchain
    user_account = user_account_key_pair.public
    treasury_account = treasury_account_key_pair.public
    treasury_initial_balance = blockchain.get_account_current_balance(treasury_account)
    assert treasury_initial_balance is not None

    assert blockchain.get_blockchain_state_by_block_number() == blockchain_genesis_state
    assert blockchain.get_blockchain_state_by_block_number(-1) == blockchain_genesis_state
    assert blockchain_genesis_state.account_states[treasury_account].balance_lock == treasury_account
    assert blockchain.get_blockchain_states_count() == 1

    blockchain.snapshot_blockchain_state()
    assert blockchain.get_blockchain_states_count() == 1

    block0 = Block.create_from_main_transaction(
        blockchain, user_account, 30, signing_key=treasury_account_key_pair.private
    )
    blockchain.add_block(block0)

    blockchain.snapshot_blockchain_state()
    assert blockchain.get_blockchain_states_count() == 2
    blockchain.snapshot_blockchain_state()
    assert blockchain.get_blockchain_states_count() == 2

    account_root_file = blockchain.get_last_blockchain_state()
    assert account_root_file is not None
    assert account_root_file.last_block_number == 0
    assert account_root_file.last_block_identifier == block0.message.block_identifier
    assert account_root_file.next_block_identifier == block0.hash

    assert len(account_root_file.account_states) == 4
    assert account_root_file.account_states.keys() == {
        user_account, treasury_account, primary_validator.identifier, preferred_node.identifier
    }
    assert account_root_file.account_states[user_account].balance == 30
    assert account_root_file.account_states[user_account].balance_lock is None
    assert account_root_file.account_states[user_account].get_balance_lock(user_account) == user_account

    assert account_root_file.account_states[treasury_account].balance == treasury_initial_balance - 30 - 4 - 1
    assert account_root_file.account_states[treasury_account].balance_lock != treasury_account

    assert account_root_file.account_states[primary_validator.identifier].balance == 4
    assert account_root_file.account_states[primary_validator.identifier].balance_lock is None
    assert account_root_file.account_states[primary_validator.identifier].get_balance_lock(
        primary_validator.identifier
    ) == primary_validator.identifier

    assert account_root_file.account_states[preferred_node.identifier].balance == 1
    assert account_root_file.account_states[preferred_node.identifier].balance_lock is None
    assert account_root_file.account_states[preferred_node.identifier].get_balance_lock(
        preferred_node.identifier
    ) == preferred_node.identifier

    block1 = Block.create_from_main_transaction(
        blockchain, treasury_account, 20, signing_key=user_account_key_pair.private
    )
    blockchain.add_block(block1)

    block2 = Block.create_from_main_transaction(
        blockchain, primary_validator.identifier, 2, signing_key=treasury_account_key_pair.private
    )
    blockchain.add_block(block2)

    blockchain.snapshot_blockchain_state()
    account_root_file = blockchain.get_last_blockchain_state()

    assert account_root_file is not None
    assert account_root_file.last_block_number == 2
    assert account_root_file.last_block_identifier == block2.message.block_identifier
    assert account_root_file.next_block_identifier == block2.hash

    assert len(account_root_file.account_states) == 4
    assert account_root_file.account_states.keys() == {
        user_account, treasury_account, primary_validator.identifier, preferred_node.identifier
    }
    assert account_root_file.account_states[user_account].balance == 5
    assert account_root_file.account_states[user_account].balance_lock != user_account

    assert account_root_file.account_states[treasury_account
                                            ].balance == treasury_initial_balance - 30 - 4 - 1 + 20 - 2 - 4 - 1
    assert account_root_file.account_states[treasury_account].balance_lock != treasury_account

    assert account_root_file.account_states[primary_validator.identifier].balance == 4 + 4 + 4 + 2
    assert account_root_file.account_states[primary_validator.identifier].balance_lock is None
    assert account_root_file.account_states[primary_validator.identifier].get_balance_lock(
        primary_validator.identifier
    ) == primary_validator.identifier

    assert account_root_file.account_states[preferred_node.identifier].balance == 1 + 1 + 1
    assert account_root_file.account_states[preferred_node.identifier].balance_lock is None
    assert account_root_file.account_states[preferred_node.identifier].get_balance_lock(
        preferred_node.identifier
    ) == preferred_node.identifier
