import copy

from thenewboston_node.business_logic.blockchain.memory_blockchain import MemoryBlockchain


def test_get_latest_account_root_file(forced_memory_blockchain: MemoryBlockchain, blockchain_genesis_state):
    blockchain_states = forced_memory_blockchain.blockchain_states
    assert blockchain_genesis_state.is_initial()
    assert blockchain_genesis_state.last_block_number is None
    assert len(blockchain_states) == 1
    assert forced_memory_blockchain.get_last_blockchain_state() == blockchain_genesis_state
    assert forced_memory_blockchain.get_first_blockchain_state() == blockchain_genesis_state

    blockchain_state1 = copy.deepcopy(blockchain_genesis_state)
    blockchain_state1.last_block_number = 3
    forced_memory_blockchain.blockchain_states.append(blockchain_state1)

    assert len(blockchain_states) == 2
    assert forced_memory_blockchain.get_last_blockchain_state() == blockchain_state1
    assert forced_memory_blockchain.get_first_blockchain_state() == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(0) == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(1) == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(2) == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(3) == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(4) == blockchain_state1

    blockchain_state2 = copy.deepcopy(blockchain_genesis_state)
    blockchain_state2.last_block_number = 5
    forced_memory_blockchain.blockchain_states.append(blockchain_state2)

    assert len(blockchain_states) == 3
    assert forced_memory_blockchain.get_last_blockchain_state() == blockchain_state2
    assert forced_memory_blockchain.get_first_blockchain_state() == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(0) == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(1) == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(2) == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(3) == blockchain_genesis_state
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(4) == blockchain_state1
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(5) == blockchain_state1
    assert forced_memory_blockchain.get_blockchain_state_by_block_number(6) == blockchain_state2
