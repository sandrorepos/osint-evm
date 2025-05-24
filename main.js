require('dotenv').config();
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');

const API_KEYS = {
  etherscan: process.env.ETHERSCAN_API_KEY,
  polygonscan: process.env.POLYGONSCAN_API_KEY,
  bscscan: process.env.BSCSCAN_API_KEY,
  arbiscan: process.env.ARBISCAN_API_KEY,
  optimistic: process.env.OPTIMISTIC_API_KEY
};

const DB_FOLDER = 'blockchain-data';

const NETWORKS = {
  ethereum: {
    name: 'Ethereum Mainnet',
    apiUrl: 'https://api.etherscan.io/api',
    explorer: 'etherscan',
    chainId: 1,
    currency: 'ETH',
    dbFile: 'ethereum.db'
  },
  polygon: {
    name: 'Polygon Mainnet',
    apiUrl: 'https://api.polygonscan.com/api',
    explorer: 'polygonscan',
    chainId: 137,
    currency: 'MATIC',
    dbFile: 'polygon.db'
  },
  bsc: {
    name: 'Binance Smart Chain',
    apiUrl: 'https://api.bscscan.com/api',
    explorer: 'bscscan',
    chainId: 56,
    currency: 'BNB',
    dbFile: 'bsc.db'
  },
  arbitrum: {
    name: 'Arbitrum One',
    apiUrl: 'https://api.arbiscan.io/api',
    explorer: 'arbiscan',
    chainId: 42161,
    currency: 'ETH',
    dbFile: 'arbitrum.db'
  },
  optimism: {
    name: 'Optimism',
    apiUrl: 'https://api-optimistic.etherscan.io/api',
    explorer: 'optimistic',
    chainId: 10,
    currency: 'ETH',
    dbFile: 'optimism.db'
  }
};

if (!fs.existsSync(DB_FOLDER)) {
  fs.mkdirSync(DB_FOLDER);
}

function initDatabase(network) {
  const dbPath = path.join(DB_FOLDER, NETWORKS[network].dbFile);
  const db = new sqlite3.Database(dbPath);

  db.serialize(() => {
    db.run(`
      CREATE TABLE IF NOT EXISTS transactions (
        hash TEXT PRIMARY KEY,
        blockNumber INTEGER,
        timeStamp INTEGER,
        fromAddress TEXT,
        toAddress TEXT,
        value TEXT,
        gas TEXT,
        gasPrice TEXT,
        gasUsed TEXT,
        transactionType TEXT,
        input TEXT,
        contractAddress TEXT,
        cumulativeGasUsed TEXT,
        nonce INTEGER,
        confirmations INTEGER,
        isError INTEGER,
        txreceipt_status TEXT,
        transactionIndex INTEGER
      )
    `);

    db.run(`
      CREATE TABLE IF NOT EXISTS addresses (
        address TEXT PRIMARY KEY,
        first_seen INTEGER,
        last_checked INTEGER,
        transaction_count INTEGER
      )
    `);

    db.run(`
      CREATE TABLE IF NOT EXISTS network_info (
        last_block INTEGER,
        last_updated INTEGER
      )
    `);
  });

  return db;
}

async function fetchTransactions(address, network) {
  const config = NETWORKS[network];
  
  try {
    console.log(`[${config.name}] Buscando transações para ${address}...`);
    
    const response = await axios.get(config.apiUrl, {
      params: {
        module: 'account',
        action: 'txlist',
        address: address,
        startblock: 0,
        endblock: 99999999,
        sort: 'asc',
        apikey: API_KEYS[config.explorer]
      }
    });

    if (response.data.status === "1") {
      return response.data.result;
    } else {
      throw new Error(response.data.message || `Erro na API ${config.name}`);
    }
  } catch (error) {
    console.error(`[${config.name}] Erro ao buscar transações: ${error.message}`);
    return [];
  }
}

async function fetchTokenTransactions(address, network) {
  const config = NETWORKS[network];
  
  try {
    console.log(`[${config.name}] Buscando transações de token...`);
    
    const response = await axios.get(config.apiUrl, {
      params: {
        module: 'account',
        action: 'tokentx',
        address: address,
        startblock: 0,
        endblock: 99999999,
        sort: 'asc',
        apikey: API_KEYS[config.explorer]
      }
    });

    if (response.data.status === "1") {
      return response.data.result;
    } else {
      throw new Error(response.data.message || `Erro na API de tokens ${config.name}`);
    }
  } catch (error) {
    console.error(`[${config.name}] Erro ao buscar transações de token: ${error.message}`);
    return [];
  }
}

async function fetchInternalTransactions(address, network) {
  const config = NETWORKS[network];
  
  try {
    console.log(`[${config.name}] Buscando transações internas...`);
    
    const response = await axios.get(config.apiUrl, {
      params: {
        module: 'account',
        action: 'txlistinternal',
        address: address,
        startblock: 0,
        endblock: 99999999,
        sort: 'asc',
        apikey: API_KEYS[config.explorer]
      }
    });

    if (response.data.status === "1") {
      return response.data.result;
    } else {
      throw new Error(response.data.message || `Erro na API interna ${config.name}`);
    }
  } catch (error) {
    console.error(`[${config.name}] Erro ao buscar transações internas: ${error.message}`);
    return [];
  }
}

function processTransactions(transactions, network, type = 'normal') {
  return transactions.map(tx => ({
    ...tx,
    transactionType: type,
    value: tx.value || '0',
    gas: tx.gas || '0',
    gasPrice: tx.gasPrice || '0',
    gasUsed: tx.gasUsed || '0',
    isError: tx.isError || '0',
    contractAddress: tx.contractAddress || null
  }));
}

async function saveTransactions(db, transactions, network) {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      const stmt = db.prepare(`
        INSERT OR IGNORE INTO transactions VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
      `);

      transactions.forEach(tx => {
        stmt.run(
          tx.hash,
          parseInt(tx.blockNumber),
          parseInt(tx.timeStamp),
          tx.from,
          tx.to,
          tx.value,
          tx.gas,
          tx.gasPrice,
          tx.gasUsed,
          tx.transactionType,
          tx.input,
          tx.contractAddress,
          tx.cumulativeGasUsed || null,
          parseInt(tx.nonce || 0),
          parseInt(tx.confirmations || 0),
          parseInt(tx.isError || 0),
          tx.txreceipt_status || null,
          parseInt(tx.transactionIndex || 0)
        );
      });

      stmt.finalize(err => {
        if (err) {
          reject(err);
        } else {
          console.log(`[${NETWORKS[network].name}] ${transactions.length} transações salvas`);
          resolve();
        }
      });
    });
  });
}

async function updateAddressInfo(db, address, transactionCount, network) {
  const now = Math.floor(Date.now() / 1000);
  
  db.run(`
    INSERT OR REPLACE INTO addresses VALUES (
      ?, 
      COALESCE((SELECT first_seen FROM addresses WHERE address = ?), ?),
      ?,
      ?
    )
  `, [address, address, now, now, transactionCount], (err) => {
    if (err) {
      console.error(`[${NETWORKS[network].name}] Erro ao atualizar endereço: ${err.message}`);
    }
  });
}

function updateNetworkInfo(db, network) {
  const now = Math.floor(Date.now() / 1000);
  
  db.run(`
    INSERT OR REPLACE INTO network_info VALUES (
      (SELECT MAX(blockNumber) FROM transactions),
      ?
    )
  `, [now], (err) => {
    if (err) {
      console.error(`[${NETWORKS[network].name}] Erro ao atualizar info da rede: ${err.message}`);
    }
  });
}

async function processNetwork(address, network) {
  const config = NETWORKS[network];
  const db = initDatabase(network);
  
  try {
    console.log(`\n[${config.name}] Iniciando processamento...`);
    
    const [normalTxs, tokenTxs, internalTxs] = await Promise.all([
      fetchTransactions(address, network),
      fetchTokenTransactions(address, network),
      fetchInternalTransactions(address, network)
    ]);

    const allTransactions = [
      ...processTransactions(normalTxs, network, 'normal'),
      ...processTransactions(tokenTxs, network, 'token'),
      ...processTransactions(internalTxs, network, 'internal')
    ];

    if (allTransactions.length === 0) {
      console.log(`[${config.name}] Nenhuma transação encontrada.`);
      return 0;
    }

    await saveTransactions(db, allTransactions, network);
    
    await updateAddressInfo(db, address, allTransactions.length, network);
    
    updateNetworkInfo(db, network);
    
    console.log(`[${config.name}] Processamento concluído.`);
    return allTransactions.length;
  } catch (error) {
    console.error(`[${config.name}] Erro no processamento:`, error);
    return 0;
  } finally {
    db.close();
  }
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.log('Uso: node multi-network-separate-dbs.js <endereço> [rede]');
    console.log('Redes suportadas:', Object.keys(NETWORKS).join(', '));
    console.log('Se nenhuma rede for especificada, buscará em todas as redes');
    process.exit(1);
  }

  const address = args[0];
  const specificNetwork = args[1] ? args[1].toLowerCase() : null;

  if (specificNetwork && !NETWORKS[specificNetwork]) {
    console.error('Rede não suportada. Redes disponíveis:', Object.keys(NETWORKS).join(', '));
    process.exit(1);
  }

  try {
    const networksToCheck = specificNetwork ? [specificNetwork] : Object.keys(NETWORKS);
    let totalTransactions = 0;

    for (const network of networksToCheck) {
      const count = await processNetwork(address, network);
      totalTransactions += count;
    }

    console.log(`\nProcesso concluído. Total de transações em todas as redes: ${totalTransactions}`);
  } catch (error) {
    console.error('Erro no processo principal:', error);
  }
}

main();
