const fs = require('fs');
const meta = JSON.parse(fs.readFileSync('metadata_new.json', 'utf8'));
const v15 = meta.metadata.v15 || {};

const targetPallets = ['Buckets', 'Marketplace', 'RealEstateNfts', 'RealWorldAsset'];

targetPallets.forEach(palletName => {
  const pallet = v15.pallets?.find(p => p.name === palletName);
  if (!pallet) {
    console.log(`\n=== ${palletName}: NOT FOUND ===\n`);
    return;
  }
  
  console.log(`\n${'='.repeat(60)}`);
  console.log(`PALLIET: ${palletName}`);
  console.log('='.repeat(60));
  
  // Events
  if (pallet.events && pallet.events.length > 0) {
    console.log(`\n--- EVENTS (${pallet.events.length}) ---`);
    pallet.events.forEach((e, i) => {
      console.log(`\n  [${i}] ${e.name}`);
      if (e.fields) {
        e.fields.forEach((f, j) => {
          const docs = f.docs ? ` "${f.docs.join(' ')}"` : '';
          console.log(`      [${j}] ${f.name || '(unnamed)'}: type=${f.type} ${docs}`);
        });
      }
    });
  } else {
    console.log('\n  NO EVENTS');
  }
  
  // Errors
  if (pallet.errors && pallet.errors.length > 0) {
    console.log(`\n--- ERRORS (${pallet.errors.length}) ---`);
    pallet.errors.forEach((e, i) => {
      console.log(`  [${i}] ${e.name}: ${e.docs?.join(' ') || 'no docs'}`);
    });
  }
  
  // Storage
  if (pallet.storage && pallet.storage.functions && pallet.storage.functions.length > 0) {
    console.log(`\n--- STORAGE (${pallet.storage.functions.length}) ---`);
    pallet.storage.functions.forEach((s, i) => {
      console.log(`  [${i}] ${s.name}`);
    });
  }
  
  // Call
  if (pallet.call && pallet.call.index !== undefined) {
    console.log(`\n--- CALL index: ${pallet.call.index} ---`);
  }
  
  // Constants
  if (pallet.constants && pallet.constants.length > 0) {
    console.log(`\n--- CONSTANTS (${pallet.constants.length}) ---`);
    pallet.constants.forEach(c => console.log(`  - ${c.name}`));
  }
});

// Also check for any custom types referenced in the lookup table
console.log('\n\n' + '='.repeat(60));
console.log('LOOKUP TYPES (first 100 for context)');
console.log('='.repeat(60));
if (v15.lookup && v15.lookup.types) {
  console.log(`Total lookup types: ${v15.lookup.types.length}`);
  // Show type IDs that might be used by our pallets
  const sampleIds = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  sampleIds.forEach(id => {
    const t = v15.lookup.types.find(t => t.id === id);
    if (t) {
      const path = t.type.path || [];
      const typeName = path[path.length - 1] || t.type.def?.primitive || '?';
      console.log(`  Type[${id}]: ${t.type.path.join('::') || 'anon'} -> ${typeName}`);
    }
  });
}