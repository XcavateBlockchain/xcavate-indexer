const fs = require('fs');
const meta = JSON.parse(fs.readFileSync('metadata_new.json', 'utf8'));
const v15 = meta.metadata.v15 || {};
const lookup = v15.lookup?.types || [];

function resolveField(typeId, depth = 0) {
  if (depth > 10) return '?';
  const t = lookup.find(x => x.id === typeId);
  if (!t) return `?[${typeId}]`;
  if (t.type.def?.primitive) return t.type.def.primitive;
  if (t.type.def?.composite?.fields) {
    return t.type.def.composite.fields.map(f => 
      `${f.name || '?' }(${resolveField(f.type, depth+1)})`
    ).join(', ');
  }
  if (t.type.def?.variant) return 'enum';
  return t.type.path?.join('::') || `?[${typeId}]`;
}

function getCompositeFields(typeId, depth = 0) {
  if (depth > 10) return [];
  const t = lookup.find(x => x.id === typeId);
  if (!t || !t.type.def?.composite?.fields) return [];
  return t.type.def.composite.fields.map(f => ({
    name: f.name,
    type: f.type,
    typeString: resolveField(f.type, depth+1)
  }));
}

// ============================================================================
// 1. ShareOwner double-map key structure (key type ID 278)
// ============================================================================

console.log('=== ShareOwner key structure (type[278]) ===\n');
const soKeyFields = getCompositeFields(278);
console.log('Composite key fields:');
soKeyFields.forEach(f => console.log(`  [${f.type}] ${f.name}: ${f.typeString}`));

// The indexer code accesses: api.query.marketplace.shareOwner(account, listingId)
// The schema entity field order in the double map should match this
console.log('\n  Indexer code calls: .shareOwner(account, listingId)');
if (soKeyFields.length === 2) {
  console.log(`  Storage key order: [0]=${soKeyFields[0].name}(${soKeyFields[0].typeString}), [1]=${soKeyFields[1].name}(${soKeyFields[1].typeString})`);
  if (soKeyFields[0].name === 'account' || soKeyFields[1].name === 'account') {
    const accountIdx = soKeyFields[0].name === 'account' ? 0 : 1;
    const listingIdx = accountIdx === 0 ? 1 : 0;
    console.log(`  ⚠️  account is key[${accountIdx}], listing is key[${listingIdx}]`);
    console.log(`  .shareOwner(account, listingId) assumes account=first key ✅`);
  }
}

// ============================================================================
// 2. OngoingOffers key structure (key type ID 266)
// ============================================================================

console.log('\n\n=== OngoingOffers key structure (type[266]) ===\n');
const ooKeyFields = getCompositeFields(266);
console.log('Composite key fields:');
ooKeyFields.forEach(f => console.log(`  [${f.type}] ${f.name}: ${f.typeString}`));

console.log('\n  Indexer code calls: .ongoingOffers(listingId, offeror)');
if (ooKeyFields.length === 2) {
  console.log(`  Storage key order: [0]=${ooKeyFields[0].name}(${ooKeyFields[0].typeString}), [1]=${ooKeyFields[1].name}(${ooKeyFields[1].typeString})`);
  const listingIdx = ooKeyFields.findIndex(f => f.name.includes('listing') || f.name.includes('index'));
  const offerorIdx = listingIdx >= 0 ? (listingIdx === 0 ? 1 : 0) : -1;
  if (listingIdx >= 0) {
    console.log(`  listing is at key[${listingIdx}], but indexer passes listingId as FIRST arg`);
    console.log(`  ${listingIdx === 0 ? '✅ CORRECT' : '❌ WRONG ORDER'} - indexer calls .ongoingOffers(listingId, offeror)`);
  }
}

// ============================================================================
// 3. UserLawyerVote key structure (key type ID 326)
// ============================================================================

console.log('\n\n=== UserLawyerVote key structure (type[326]) ===\n');
const ulvKeyFields = getCompositeFields(326);
console.log('Composite key fields:');
ulvKeyFields.forEach(f => console.log(`  [${f.type}] ${f.name}: ${f.typeString}`));

console.log('\n  Indexer code calls: .userLawyerVote(proposalId, voter)');
if (ulvKeyFields.length === 2) {
  console.log(`  Storage key order: [0]=${ulvKeyFields[0].name}(${ulvKeyFields[0].typeString}), [1]=${ulvKeyFields[1].name}(${ulvKeyFields[1].typeString})`);
}

// ============================================================================
// 4. ShareOwner value structure (type 362)
// ============================================================================

console.log('\n\n=== ShareOwner value structure (type[362]) ===\n');
const soValueFields = getCompositeFields(362);
console.log('Value fields:');
soValueFields.forEach(f => console.log(`  ${f.name}: ${f.typeString}`));

console.log('\n  Schema: MarketplaceShareOwners { tokenAmount, paidFunds, paidTax, relistCount, updatedBlock }');
console.log('  Code reads: token_amount, paid_funds, paid_tax, relist_count');

// Check alignment
const codeFields = ['token_amount', 'paid_funds', 'paid_tax', 'relist_count'];
const metadataFieldNames = soValueFields.map(f => f.name);
console.log('\n  Field alignment check:');
codeFields.forEach(cf => {
  const found = metadataFieldNames.find(mf => mf.toLowerCase() === cf || mf.toLowerCase().replace(/[_\s]/g,'') === cf.replace(/[_\s]/g,''));
  console.log(`    ${cf}: ${found ? '✅ ' + found : '❌ NOT FOUND'}`);
});

// ============================================================================
// 5. PropertyListingDetails value structure (type 356)
// ============================================================================

console.log('\n\n=== OngoingObjectListing value structure (type[356]) ===\n');
const olValueFields = getCompositeFields(356);
console.log('Value fields:');
olValueFields.forEach(f => console.log(`  ${f.name}: ${f.typeString}`));

console.log('\n  Schema: MarketplaceOngoingObjectListings { listingId, tokenPrice, tokenAmount, ... }');
console.log('  Code reads: token_price, token_amount, listed_token_amount, tax_paid_by_developer, tax, listing_expiry, claim_expiry, relist_count, unclaimed_token_amount, collected_funds, collected_tax, collected_fees, investor_funds');

console.log('\n  Field alignment check:');
const olCodeFields = [
  { meta: 'real_estate_developer', schema: 'realEstateDeveloper' },
  { meta: 'share_price', schema: 'tokenPrice' },
  { meta: 'collected_funds', schema: 'collectedFunds' },
  { meta: 'collected_tax', schema: 'collectedTax' },
  { meta: 'collected_fees', schema: 'collectedFees' },
  { meta: 'asset_id', schema: 'assetId' },
  { meta: 'item_id', schema: 'itemId' },
  { meta: 'collection_id', schema: 'collectionId' },
  { meta: 'share_amount', schema: 'tokenAmount' },
  { meta: 'listed_share_amount', schema: 'listedTokenAmount' },
  { meta: 'tax_paid_by_developer', schema: 'taxPaidByDeveloper' },
  { meta: 'tax', schema: 'tax' },
  { meta: 'listing_expiry', schema: 'listingExpiry' },
  { meta: 'claim_expiry', schema: 'claimExpiry' },
  { meta: 'relist_count', schema: 'relistCount' },
  { meta: 'unclaimed_share_amount', schema: 'unclaimedTokenAmount' },
];
const olMetaNames = olValueFields.map(f => f.name);
olCodeFields.forEach(cf => {
  const found = olMetaNames.find(m => m === cf.meta);
  if (found) {
    console.log(`  ✅ ${cf.meta} → schema ${cf.schema}`);
  } else {
    console.log(`  ❌ ${cf.meta} NOT FOUND in metadata!`);
  }
});

// Check for fields in metadata NOT in code:
console.log('\n  Metadata fields NOT read by indexer:');
olMetaNames.forEach(mn => {
  if (!olCodeFields.find(cf => cf.meta === mn)) {
    const mt = olValueFields.find(f => f.name === mn);
    console.log(`  - ${mn} (${mt?.typeString}) - not read by syncOngoingObjectListing`);
  }
});

// ============================================================================
// 6. Check Refund-related storage that has NO handler coverage
// ============================================================================

console.log('\n\n=== MARKETPLACE REFT STORAGE WITH NO HANDLERS ===\n');
if (marketplacePallet?.storage?.items) {
  const refundItems = marketplacePallet.storage.items.filter(i => 
    i.name.toLowerCase().includes('refund')
  );
  refundItems.forEach(item => {
    console.log(`  ${item.name}: key=${item.type.map.key}(${resolveField(item.type.map.key)}), value=${item.type.map.value}(${resolveField(item.type.map.value)})`);
  });
  
  console.log('\n  These storage keys have NO handler in marketplace.ts:');
  refundItems.forEach(item => console.log(`    - ${item.name}`));
}

// ============================================================================
// 7. Check OngoingOffers value structure
// ============================================================================

console.log('\n\n=== OngoingOffers value structure ===\n');
const ooPallet = v15.pallets.find(p => p.name === 'Marketplace');
const ooItem = ooPallet?.storage?.items?.find(i => i.name === 'OngoingOffers');
if (ooItem && ooItem.type.map.value) {
  const ooValueFields = getCompositeFields(ooItem.type.map.value);
  console.log('OngoingOffers value fields:');
  ooValueFields.forEach(f => console.log(`  ${f.name}: ${f.typeString}`));
}

// 8. Check ShareOwner value fields for mismatch
console.log('\n\n=== MARKETPLACE SHAREOWNER DETAILS STRUCTURE ===\n');
// From earlier: value type is 362
const soDetailFields = getCompositeFields(362);
console.log('ShareOwnerDetails fields:');
soDetailFields.forEach(f => console.log(`  ${f.name}: ${f.typeString}`));