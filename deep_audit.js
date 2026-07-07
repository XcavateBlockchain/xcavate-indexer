const fs = require('fs');
const meta = JSON.parse(fs.readFileSync('metadata_new.json', 'utf8'));
const v15 = meta.metadata.v15 || {};
const lookup = v15.lookup?.types || [];

// Helper to resolve type info
function getTypeInfo(typeId) {
  const t = lookup.find(x => x.id === typeId);
  return t ? { id: t.id, path: t.type.path?.join('::'), def: t.type.def } : null;
}

function getBaseType(typeId) {
  const info = getTypeInfo(typeId);
  if (!info) return `type[${typeId}]`;
  if (info.def?.primitive) return info.def.primitive;
  if (info.def?.composite) {
    // Find first primitive field
    for (const f of (info.def.composite.fields || [])) {
      const pt = getBaseType(f.type);
      if (pt) return pt;
    }
  }
  if (info.def?.variant) return 'enum';
  return `type[${typeId}] (${info.path || 'anon'})`;
}

// ============================================================================
// 1. CHECK ALL STORAGE ITEMS FOR EACH TARGET PALLET
// ============================================================================

console.log('=== STORAGE ITEMS FOR TARGET PALLETS ===\n');

const targetPallets = ['Buckets', 'Marketplace', 'RealWorldAsset', 'RealEstateNfts'];

targetPallets.forEach(palletName => {
  const pallet = v15.pallets.find(p => p.name === palletName);
  if (!pallet) { console.log(palletName + ': NOT FOUND'); return; }
  
  console.log(`\n--- ${palletName} (${pallet.storage?.prefix}) ---`);
  
  if (pallet.storage?.items) {
    pallet.storage.items.forEach(item => {
      const hashers = item.type.map?.hashers?.join(',') || (item.type.plain ? 'plain' : '?');
      const mapKeyType = item.type.map?.key != null ? `key[${item.type.map.key}] (${getBaseType(item.type.map.key)})` : 'N/A';
      const mapValueType = item.type.map?.value != null ? `value[${item.type.map.value}] (${getBaseType(item.type.map.value)})` : 'N/A';
      
      console.log(`  ${item.name}: hashers=[${hashers}], key=${mapKeyType}, value=${mapValueType}`);
      
      // Check for u64/u128 keys that should be Int
      if (item.type.map?.key != null) {
        const keyBase = getBaseType(item.type.map.key);
        if (keyBase === 'U64') {
          console.log(`    -> u64 key type (SubQuery GraphQL int max is 32-bit)`);
        }
      }
    });
  }
});

// ============================================================================
// 2. CHECK SCHEMA TYPES vs METADATA TYPES FOR MISMATCHES
// ============================================================================

console.log('\n\n=== SCHEMA ENTITY FIELD MISMATCH CHECK ===\n');

// Check u64/u128 → GraphQL Int overflow risk
console.log('--- GraphQL Int overflow risk (u64/u128 in metadata → Int in schema) ---');

// Buckets storage keys
console.log('\nBuckets storage:');
const bucketsPallet = v15.pallets.find(p => p.name === 'Buckets');
if (bucketsPallet?.storage?.items) {
  bucketsPallet.storage.items.forEach(item => {
    if (item.type.map?.key != null) {
      const keyType = getBaseType(item.type.map.key);
      if (keyType === 'U64') {
        console.log(`  ${item.name}: key is ${keyType} — GraphQL Int may overflow`);
      }
    }
    if (item.type.map?.value != null) {
      const valType = getBaseType(item.type.map.value);
      if (valType === 'U128' || valType === 'U64') {
        console.log(`  ${item.name}: value is ${valType} — GraphQL Int may overflow`);
      }
    }
  });
}

// ============================================================================
// 3. CHECK EVENT FIELD TYPE ALIGNMENT WITH HANDLER LOGIC
// ============================================================================

console.log('\n\n=== EVENT FIELD TYPE ANALYSIS ===\n');

// Buckets events
const bucketsEventTypeId = 99;
const bucketsEventType = lookup.find(x => x.id === bucketsEventTypeId);
if (bucketsEventType?.type.def?.variant) {
  console.log('Buckets event type field types:');
  bucketsEventType.type.def.variant.variants.forEach(v => {
    console.log(`  ${v.name}:`);
    v.fields?.forEach(f => {
      const fType = getBaseType(f.type);
      console.log(`    [${f.name}]: ${fType}`);
    });
  });
}

// Check which Buckets storage items are NOT covered in indexer code
console.log('\n\n=== STORAGE ITEMS NOT COVERED IN INDEXER ===\n');

if (bucketsPallet?.storage?.items) {
  const knownIndexerKeys = ['buckets', 'contributors', 'admins', 'messages'];
  const covered = new Set(knownIndexerKeys);
  
  bucketsPallet.storage.items.forEach(item => {
    if (!covered.has(item.name.toLowerCase())) {
      console.log(`  ${item.name}: NOT covered in indexer storage sync`);
    }
  });
}

// ============================================================================
// 4. CHECK FOR DOUBLE-MAP STORAGE KEY ORDER MISMATCHES
// ============================================================================

console.log('\n\n=== DOUBLE-MAP STORAGE KEY ORDER CHECK ===\n');

[targetPallets, 'Marketplace'].forEach(palletName => {
  const pallet = v15.pallets.find(p => p.name === palletName);
  if (!pallet || !pallet.storage?.items) return;
  
  pallet.storage.items.forEach(item => {
    if (item.type.map?.hashers?.length === 2) {
      const key1Type = getBaseType(item.type.map.key);
      const key2Type = getBaseType(item.type.map.key);
      console.log(`  ${palletName}.${item.name}: double-map, key1=${key1Type}, key2=?`);
      console.log(`    -> Need to verify code passes args in correct order to api.query`);
    }
  });
});

// ============================================================================
// 5. CHECK EVENT FIELD COUNT vs HANDLER ARG COUNT
// ============================================================================

console.log('\n\n=== EVENT FIELD COUNT CHECK ===\n');

// Check a few key events
const keyEvents = [
  { pallet: 'Buckets', name: 'ContributorAdded', expectedArgs: 4, codeArgs: 3 },
  { pallet: 'Buckets', name: 'AdminAdded', expectedArgs: 4, codeArgs: 3 },
  { pallet: 'Buckets', name: 'ContributorRemoved', expectedArgs: 4, codeArgs: 3 },
  { pallet: 'Buckets', name: 'AdminRemoved', expectedArgs: 4, codeArgs: 3 },
  { pallet: 'Buckets', name: 'BucketCreated', expectedArgs: 4, codeArgs: '2-4 (2 shapes)' },
  { pallet: 'Buckets', name: 'NewMessage', expectedArgs: 5, codeArgs: '3-5 (2 shapes)' },
];

keyEvents.forEach(e => {
  if (e.codeArgs !== e.expectedArgs) {
    console.log(`  ${e.pallet}.${e.name}: metadata=${e.expectedArgs} fields, code handles ${e.codeArgs} — ${e.codeArgs !== e.expectedArgs ? 'MISMATCH' : 'OK'}`);
  }
});

// ============================================================================
// 6. CHECK FOR MISSING CROSS-REFERENCES
// ============================================================================

console.log('\n\n=== CROSS-REFERENCE CHECKS ===\n');

// Check if RealWorldAsset references correct NFT collection
console.log('RealWorldAsset.storage -> RealEstateNfts:');
const rwaPallet = v15.pallets.find(p => p.name === 'RealWorldAsset');
if (rwaPallet?.storage?.items) {
  rwaPallet.storage.items.forEach(item => {
    if (item.name === 'PropertyAssetInfo') {
      const valType = getBaseType(item.type.map?.value);
      console.log(`  PropertyAssetInfo value type: ${valType}`);
      // Check if it contains collection_id, item_id fields
      const valTypeInfo = getTypeInfo(item.type.map?.value);
      if (valTypeInfo?.def?.composite?.fields) {
        valTypeInfo.def.composite.fields.forEach(f => {
          const fBase = getBaseType(f.type);
          console.log(`    field: ${f.name} (${fBase})`);
        });
      }
    }
  });
}

// Check Marketplace storage for listing cross-refs
console.log('\nMarketplace.storage -> RealEstateNfts / RealWorldAsset:');
const marketplacePallet = v15.pallets.find(p => p.name === 'Marketplace');
if (marketplacePallet?.storage?.items) {
  marketplacePallet.storage.items.forEach(item => {
    if (item.name === 'OngoingObjectListing') {
      const valType = getBaseType(item.type.map?.value);
      console.log(`  OngoingObjectListing value type: ${valType}`);
      const valTypeInfo = getTypeInfo(item.type.map?.value);
      if (valTypeInfo?.def?.composite?.fields) {
        valTypeInfo.def.composite.fields.forEach(f => {
          const fBase = getBaseType(f.type);
          console.log(`    field: ${f.name} (${fBase})`);
        });
      }
    }
  });
}

// ============================================================================
// 7. CHECK FOR TYPE DISCREPANCIES BETWEEN EVENT AND STORAGE
// ============================================================================

console.log('\n\n=== EVENT vs STORAGE TYPE CONSISTENCY ===\n');

// PropertySharesBought event: listing_index is u32, but what about the storage?
console.log('PropertySharesBought [Marketplace]:');
console.log('  listing_index: u32 (args[0])');
console.log('  asset_id: u32 (args[1])');
console.log('  buyer: AccountId32 (args[2])');
console.log('  amount_purchased: u32 (args[3])');
console.log('  price_paid: u128 (args[4])');
console.log('  tax_paid: u128 (args[5])');
console.log('  payment_asset: u32 (args[6])');
console.log('  new_shares_remaining: u32 (args[7])');
console.log('\n  Code calls syncTokenOwnerFromEvent(args, 0, 2)');
console.log('    → reads args[0]=listingId ✅');
console.log('    → reads args[2]=account (buyer) ✅');
console.log('\n  But then calls api.query.marketplace.shareOwner(account, listingId)');
console.log('    → shareOwner storage key order: [account, listing_id] (double map)');
console.log('    → Polkadot API decorates: .shareOwner(account, listingId) ✅');