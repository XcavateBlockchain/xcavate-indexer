const { ApiPromise, WsProvider } = require('@polkadot/api');
const fs = require('fs');

async function main() {
  const wsProvider = new WsProvider('wss://xcavate-solochain.api.onfinality.io/ws?apikey=2214b52d-2332-4313-842f-c8434eeaada0');
  const api = await ApiPromise.create({ provider: wsProvider });
  
  // Raw metadata object
  const rawMeta = api.runtimeMetadata;
  
  // Convert to JSON
  const metaJson = rawMeta.toJSON ? rawMeta.toJSON() : rawMeta;
  
  fs.writeFileSync('metadata_new.json', JSON.stringify(metaJson, null, 2));
  console.log('Metadata saved to metadata_new.json');
  
  // Print basic structure
  if (metaJson.pallets) {
    console.log(`\nTotal pallets: ${metaJson.pallets.length}`);
    console.log('\nPallet names:');
    metaJson.pallets.forEach(p => {
      console.log(`  - ${p.name}`);
    });
  }
  
  // Check target pallets
  const targetPallets = ['buckets', 'marketplace', 'realestatenfts', 'realworldasset'];
  targetPallets.forEach(name => {
    const found = metaJson.pallets?.find(p => p.name.toLowerCase() === name.toLowerCase());
    if (found) {
      console.log(`\n=== Pallet: ${name} ===`);
      console.log(`Events: ${found.events?.length || 0}`);
      if (found.events) {
        found.events.forEach(e => console.log(`  - ${e.name}`));
      }
      if (found.storage) {
        console.log(`Storage prefix: ${found.storage.prefix}`);
      }
    } else {
      console.log(`\n=== Pallet NOT found: ${name} ===`);
    }
  });
  
  api.disconnect();
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});