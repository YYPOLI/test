# Excluded Large Spender Addresses

The following 36 spender addresses are excluded from the repository due to
their large transaction history size (>5 MB each, ~3.2 GB total). These are
well-known benign protocol contracts (Uniswap Permit2, Aave, Lido, Ether.fi,
Puffer Finance, etc.) with high transaction volumes and no malicious activity.

To obtain their history data, run:
```bash
python pipeline/data_processing/spender_history_crawler.py
```

| Address | Size (MB) | Label | Nametag |
|---|---|---|---|
| `0x4095f064b8d3c3548a3bebfd0bbfd04750e30077` | 383.3 | Benign | morpho: eth bundler v2 |
| `0x00000000009726632680fb29d3f7a9734e3010e2` | 352.2 | Benign | rainbow: router |
| `0x9f452b7cc24e6e6fa690fe77cf5dd2ba3dbf1ed9` | 299.1 | Unknown |  |
| `0x19b5cc75846bf6286d599ec116536a333c4c2c14` | 283.3 | Benign | fuel: predeposits proxy |
| `0xadc0a53095a0af87f3aa29fe0715b5c28016364e` | 190.7 | Benign | aave: swap collateral adapter v3 |
| `0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee` | 175.6 | Benign | ether.fi: weeth token |
| `0x37d7f26405103c9bc9d8f9352cf32c5b655cbe02` | 146.2 | Unknown |  |
| `0x135896de8421be2ec868e0b811006171d9df802a` | 137.8 | Benign | aave: collateral switch v2 |
| `0x000000000022d473030f116ddee9f6b43ac78ba3` | 136.1 | Benign | uniswap protocol: permit2 |
| `0x889edc2edab5f40e902b864ad4d7ade8e412f9b1` | 133.0 | Benign | lido: withdrawal queue |
| `0x308861a430be4cce5502d0a12724771fc6daf216` | 121.0 | Benign | ether.fi: liquidity pool |
| `0x4aa799c5dfc01ee7d790e3bf1a7c2257ce1dceff` | 112.0 | Benign | puffer finance: deposit |
| `0x4da27a545c0c5b758a6ba100e3a049001de870f5` | 92.5 | Benign | aave: staked aave |
| `0x6759acd57cb5ea451a3edf397734edddfc123049` | 82.7 | Benign | transparent upgradeable proxy |
| `0x35bb522b102326ea3f1141661df4626c87000e3e` | 72.6 | Benign | aave: repay with collateral v3 |
| `0x0000000000000000000000000000000000000001` | 62.4 | Benign | null: 0x000...001 |
| `0x0000000000bbf5c5fd284e657f01bd000933c96d` | 60.4 | Benign | paraswap: delta v2 |
| `0xe3cbd06d7dadb3f4e6557bab7edd924cd1489e8f` | 40.1 | Benign | mantle: lsp staking |
| `0xf7b6b32492c2e13799d921e84202450131bd238b` | 38.3 | Benign | puffer finance: puffer protocol |
| `0x35d8949372d46b7a3d5a56006ae77b215fc69bc0` | 31.9 | Benign | usual: usd0++ token |
| `0xc3bb52e6118f05dd8ad4e1c1a1398281cd7c4c7f` | 25.9 | Benign | aevo: l1 deposit helper |
| `0x6a000f20005980200259b80c5102003040001068` | 24.3 | Benign | velora: augustus v6.2 |
| `0xc13e21b648a5ee794902342038ff3adab66be987` | 22.6 | Benign | spark: sparklend |
| `0x6fc79ddd0379fd11d67dd73056465e822b843d13` | 20.6 | Unknown |  |
| `0x06b964d96f5dcf7eae9d7c559b09edce244d4b8e` | 18.7 | Benign | usual: usualx token |
| `0xb748952c7bc638f31775245964707bcc5ddfabfc` | 16.0 | Benign | aave: migration helper mainnet v3 |
| `0xed9d63a96c27f87b07115b56b2e3572827f21646` | 15.8 | Benign | rhino.fi: starkex proxy/interface |
| `0xdda0483184e75a5579ef9635ed14baccf9d50283` | 15.6 | Unknown |  |
| `0xc45e939ca8c43822a2a233404ecf420712084c30` | 8.7 | Unknown |  |
| `0xb969b0d14f7682baf37ba7c364b351b830a812b2` | 8.1 | Benign | usual: swapper engine |
| `0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d` | 8.1 | Benign | aave: staked gho |
| `0x9ffdf407cde9a93c47611799da23924af3ef764f` | 8.0 | Benign | ether.fi: vampire |
| `0x134ccaaa4f1e4552ec8aecb9e4a2360ddcf8df76` | 6.8 | Benign | syrup: usdc router |
| `0x3f37c7d8e61c000085aac0515775b06a3412f36b` | 6.4 | Unknown |  |
| `0x75b7b44dcd28df0beda913ae2809ede986e8f461` | 5.6 | Benign | syrup: mpl user actions |
| `0x21dd761cac8461a68344f40d2f12e172a18a297f` | 5.1 | Unknown |  |
