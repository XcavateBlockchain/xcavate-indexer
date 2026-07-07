const fs = require('fs');
const meta = JSON.parse(fs.readFileSync('metadata_new.json', 'utf8'));
const v15 = meta.metadata.v15 || {};
const lookup = v15.lookup?.types || [];

// ============================================================================
// 1. REALWORLDASSET PROPERTYASSETINFO VALUE TYPE CHECK
// ============================================================================

console.log('=== PropertyAssetInfo VALUE TYPE ANALYSIS ===\n');

const rwaPallet = v15.pallets.find(p => p.name === 'RealWorldAsset');
if (rwaPallet?.storage?.items) {
  const propAsset = rwaPallet.storage.items.find(i => i.name === 'PropertyAssetInfo');
  if (propAsset) {
    console.log(`Storage: PropertyAssetInfo`);
    console.log(`  Value type ID: ${propAsset.type.map.value}`);
    const valType = lookup.find(t => t.id === propAsset.type.map.value);
    if (valType) {
      console.log(`  Resolved: ${valType.type.path?.join('::')}`);
      console.log(`  Def type: ${valType.type.def?.primitive || valType.type.def?.composite ? (valType.type.def?.composite ? 'composite' : valType.type.def.primitive) : 'variant/other'}`);
      if (valType.type.def?.composite?.fields) {
        console.log(`  Fields:`);
        valType.type.def.composite.fields.forEach(f => {
          const inner = lookup.find(x => x.id === f.type);
          console.log(`    ${f.name}: ${inner?.type.path?.join('::') || 'type[' + f.type + ']'}`);
        });
      } else if (valType.type.def?.primitive) {
        console.log(`  Primitive: ${valType.type.def.primitive}`);
      } else if (valType.type.def?.variant) {
        console.log(`  Variant (enum)`);
        valType.type.def.variant.variants.forEach(v => {
          console.log(`    ${v.name}`);
        });
      }
    }
  }
}

// ============================================================================
// 2. OngoingObjectListing VALUE TYPE
// ============================================================================

console.log('\n\n=== OngoingObjectListing VALUE TYPE ANALYSIS ===\n');

const marketplacePallet = v15.pallets.find(p => p.name === 'Marketplace');
if (marketplacePallet?.storage?.items) {
  const ongoingObj = marketplacePallet.storage.items.find(i => i.name === 'OngoingObjectListing');
  if (ongoingObj) {
    console.log(`Storage: OngoingObjectListing`);
    console.log(`  Value type ID: ${ongoingObj.type.map.value}`);
    const valType = lookup.find(t => t.id === ongoingObj.type.map.value);
    if (valType) {
      console.log(`  Resolved: ${valType.type.path?.join('::')}`);
      if (valType.type.def?.composite?.fields) {
        valType.type.def.composite.fields.forEach(f => {
          const inner = lookup.find(x => x.id === f.type);
          console.log(`    ${f.name}: ${inner?.type.path?.join('::') || 'type[' + f.type + ']'}`);
        });
      }
    }
  }
}

// ============================================================================
// 3. ShareOwner storage key order
// ============================================================================

console.log('\n\n=== ShareOwner DOUBLE-MAP KEY ORDER ===\n');

const shareOwner = marketplacePallet?.storage?.items?.find(i => i.name === 'ShareOwner');
if (shareOwner) {
  console.log(`Storage: ShareOwner`);
  console.log(`  Key type ID: ${shareOwner.type.map.key}`);
  // For double maps, there's a special structure
  if (shareOwner.type.map?.hashers?.length === 2) {
    // The metadata shows a single key field, but for double maps in v15,
    // the key is a composite of two values
    const keyType = lookup.find(t => t.id === shareOwner.type.map.key);
    if (keyType) {
      console.log(`  Key type: ${keyType.type.path?.join('::')}`);
      if (keyType.type.def?.composite?.fields) {
        console.log(`  Key composite fields:`);
        keyType.type.def.composite.fields.forEach(f => {
          const inner = lookup.find(x => x.id === f.type);
          console.log(`    [${f.index || '?'}] ${f.name}: ${inner?.type.path?.join('::') || 'type[' + f.type + ']'}`);
        });
      }
    }
    console.log(`  Value type: ${shareOwner.type.map.value} (${getBaseType(lookup, shareOwner.type.map.value)})`);
  }
}

function getBaseType(lookup, typeId) {
  const t = lookup.find(x => x.id === typeId);
  if (!t) return `type[${typeId}]`;
  if (t.type.def?.primitive) return t.type.def.primitive;
  return `(${t.type.path?.join('::') || 'anon'})`;
}

// ============================================================================
// 4. OngoingOffers key order  
// ============================================================================

console.log('\n\n=== OngoingOffers DOUBLE-MAP KEY ORDER ===\n');

const offers = marketplacePallet?.storage?.items?.find(i => i.name === 'OngoingOffers');
if (offers && offers.type.map?.hashers?.length === 2) {
  const keyType = lookup.find(t => t.id === offers.type.map.key);
  if (keyType) {
    console.log(`  Key type: ${keyType.type.path?.join('::')}`);
    if (keyType.type.def?.composite?.fields) {
      keyType.type.def.composite.fields.forEach(f => {
        const inner = lookup.find(x => x.id === f.type);
        console.log(`    [${f.index || '?'}] ${f.name}: ${inner?.type.path?.join('::') || 'type[' + f.type + ']'}`);
      });
    }
  }
}

// ============================================================================
// 5. Compare handler storage calls against actual metadata storage keys
// ============================================================================

console.log('\n\n=== HANDLER STORAGE CALLS vs METADATA ===\n');

// RealWorldAsset handler calls:
console.log('RealWorldAssets handler storage calls:');
console.log('  .propertyAssetInfo(assetId)');
console.log('  .propertyOwner(assetId)');
console.log('  .propertyOwnerShares(assetId, account)');

// Check metadata storage functions
const rwaStorageFuncs = rwaPallet?.storage?.functions || [];
console.log('  Metadata storage functions:');
rwaStorageFuncs.forEach(f => console.log(`    - ${f.name}`));

// Check case matching
const camelMatch = rwaStorageFuncs.find(f => f.name.toLowerCase() === 'propertyassetinfo');
console.log(`  propertyAssetInfo matches: ${camelMatch ? camelMatch.name : 'NONE'}`);
console.log(`  propertyOwner matches: ${rwaStorageFuncs.find(f => f.name.toLowerCase() === 'propertyowner')?.name || 'NONE'}`);
console.log(`  propertyOwnerShares matches: ${rwaStorageFuncs.find(f => f.name.toLowerCase() === 'propertyownershares')?.name || 'NONE'}`);

console.log('\n\n=== MARKETPLACE STORAGE CALLS vs METADATA ===\n');

// Marketplace storage functions from metadata
console.log('Marketplace storage functions:');
const mktStorageFuncs = marketplacePallet?.storage?.functions || [];
mktStorageFuncs.forEach(f => console.log(`    - ${f.name}`));

// Check what the handler tries to access
const handlerCalls = ['OngoingObjectListing', 'ongoingObjectListing', 'ShareListings', 'shareListings', 
  'PropertyLawyer', 'propertyLawyer', 'ListingSpvProposal', 'listingSpvProposal',
  'OngoingLawyerVoting', 'ongoingLawyerVoting', 'UserLawyerVote', 'userLawyerVote',
  'ShareOwner', 'shareOwner', 'OngoingOffers', 'ongoingOffers'];

handlerCalls.forEach(call => {
  const match = mktStorageFuncs.find(f => f.name === call);
  if (match) {
    console.log(`  ✅ ${call} → matches metadata "${match.name}"`);
  }
});

// Check for missing handler calls
console.log('\n  Handler code also calls directly via api.query.marketplace.*:');
const directCalls = ['ongoingObjectListing', 'shareListings', 'propertyLawyer', 
  'listingSpvProposal', 'ongoingLawyerVoting', 'userLawyerVote',
  'shareOwner', 'ongoingOffers'];

directCalls.forEach(call => {
  const match = mktStorageFuncs.find(f => f.name === call);
  const matchCamel = mktStorageFuncs.find(f => f.name === call.charAt(0).toUpperCase() + call.slice(1));
  if (match || matchCamel) {
    console.log(`  ✅ api.query.marketplace.${call}`);
  }
});