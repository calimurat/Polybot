"""
WALLET MANAGER — loads the private key and derives the wallet address.
Supports: MetaMask (EOA, type 0) + Magic.link (POLY_1271, type 3)

No signing here. The order signer that used to live in this file built
CLOB V1 orders (nonce/feeRateBps/taker fields, EIP-712 domain version "1",
a verifyingContract that was never the exchange's), which the exchange has
rejected since CLOB V2 went live on 2026-04-28. Nothing called it, nor the
transaction/message signers next to it; all three were removed 2026-09-29
(backups/pre_market_params_cleanup_2026-09-29 has the last copy). Real
orders must be signed by Polymarket's own SDK (py-clob-client-v2), which
tracks the exchange's contracts and order format.
"""

import logging
from typing import Dict
from dataclasses import dataclass
from eth_account import Account

logger = logging.getLogger("WalletManager")

# ============================================================================
# ENUMS
# ============================================================================

class SignatureType:
    """Polymarket signature types"""
    EOA = 0                    # MetaMask, standalone wallet
    POLY_PROXY = 1             # Old Magic.link (deprecated)
    GNOSIS_SAFE = 2            # Multi-sig wallets
    POLY_1271 = 3              # New Magic.link (deposit wallet)

class WalletType:
    """Wallet types"""
    METAMASK = "metamask"      # EOA
    MAGIC_LINK = "magic_link"  # POLY_1271
    GNOSIS_SAFE = "gnosis_safe"  # Multi-sig

# ============================================================================
# CONFIG
# ============================================================================

@dataclass
class WalletConfig:
    """Wallet configuration"""
    private_key: str            # Hex format: 0x...
    wallet_type: str            # metamask, magic_link, gnosis_safe
    signature_type: int         # 0, 1, 2, 3
    chain_id: int = 137         # Polygon mainnet
    
    def validate(self):
        """Validate config"""
        if not self.private_key or not self.private_key.startswith("0x"):
            raise ValueError("Private key must be hex format: 0x...")
        
        if self.wallet_type not in [WalletType.METAMASK, WalletType.MAGIC_LINK, WalletType.GNOSIS_SAFE]:
            raise ValueError(f"Invalid wallet type: {self.wallet_type}")
        
        if self.signature_type not in [0, 1, 2, 3]:
            raise ValueError(f"Invalid signature type: {self.signature_type}")

# ============================================================================
# WALLET MANAGER
# ============================================================================

class WalletManager:
    """Load the private key (eth-account) and expose the wallet address."""
    
    def __init__(self, config: WalletConfig):
        self.config = config
        self.config.validate()
        
        # Load account from private key
        try:
            self.account = Account.from_key(config.private_key)
            logger.info(f"[WALLET] Loaded: {self.account.address}")
        except Exception as e:
            logger.error(f"[WALLET] Failed to load private key: {e}")
            raise ValueError(f"Invalid private key: {e}")
        
        # Store wallet info. The key itself stays only inside self.account.
        self.address = self.account.address
        self.wallet_type = config.wallet_type
        self.signature_type = config.signature_type
        self.chain_id = config.chain_id
    
    # ========================================================================
    # ADDRESS OPERATIONS
    # ========================================================================
    
    def get_address(self) -> str:
        """Get wallet address"""
        return self.address
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def get_signature_type_name(self) -> str:
        """Get signature type name"""
        types = {
            0: "EOA (MetaMask)",
            1: "POLY_PROXY (Old Magic.link)",
            2: "GNOSIS_SAFE (Multi-sig)",
            3: "POLY_1271 (Magic.link deposit)"
        }
        return types.get(self.signature_type, "Unknown")
    
    def export_stats(self) -> Dict[str, int]:
        """Export statistics"""
        return {
            "wallet_address": self.address,
            "wallet_type": self.wallet_type,
            "signature_type": self.get_signature_type_name(),
        }
