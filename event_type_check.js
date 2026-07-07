const fs = require('fs');
const meta = JSON.parse(fs.readFileSync('metadata_new.json', 'utf8'));
const v15 = meta.metadata.v15 || {};
const lookup = v15.lookup?.types || [];

// Event type IDs (from earlier lookup)
const eventIds = {
  'RuntimeEvent': 416,       // Global event enum
  'RealWorldAsset': 80,
  'Marketplace': 81,
  'Buckets': 99,
  'RealEstateNfts (NFTs)': 50,
};

// Recursively resolve a type ID
function resolveType(typeId, depth = 0) {
  if (depth > 10) return '(too deep)';
  const t = lookup.find(x => x.id === typeId);
  if (!t) return `unknown[${typeId}]`;
  
  const path = t.type.path?.join('::') || '';
  if (t.type.def?.variant) {
    return t.type.def.variant.variants.map(v => {
      const fields = v.fields?.map(f => {
        const inner = resolveType(f.type, depth + 1);
        return `${f.name}(${inner})`;
      }).join(', ');
      return `${v.name}{${fields || ''}}`;
    }).join(' | ');
  }
  if (t.type.def?.composite) {
    return t.type.def.composite.fields.map(f => 
      `${f.name}(${resolveType(f.type, depth + 1)})`
    ).join(', ');
  }
  if (t.type.def?.primitive) return t.type.def.primitive;
  return `anon[${typeId}]`;
}

console.log('=== EVENT DEFINITIONS FROM LOOKUP TABLE ===\n');

Object.entries(eventIds).forEach(([name, typeId]) => {
  const t = lookup.find(x => x.id === typeId);
  if (!t) { console.log(`${name}: type[${typeId}] NOT FOUND`); return; }
  
  console.log(`\n--- ${name} (type[${typeId}]: ${t.type.path?.join('::')}) ---`);
  
  if (t.type.def?.variant) {
    console.log('VARIANT VARIANTS:');
    t.type.def.variant.variants.forEach(v => {
      const fields = v.fields?.map(f => {
        const innerT = lookup.find(x => x.id === f.type);
        const innerPath = innerT?.type.path?.join('::') || `type[${f.type}]`;
        return `${f.name}: ${innerPath}`;
      }).join(', ');
      console.log(`  [${v.index}] ${v.name} { ${fields || 'no fields'} }`);
    });
  } else if (t.type.def?.composite) {
    console.log('COMPOSITE:');
    t.type.def.composite.fields.forEach(f => {
      const innerT = lookup.find(x => x.id === f.type);
      console.log(`  ${f.name}: ${innerT?.type.path?.join('::') || 'type[' + f.type + ']'}`);
    });
  } else if (t.type.def?.primitive) {
    console.log('PRIMITIVE:', t.type.def.primitive);
  }
});

// Also check the global RuntimeEvent (type 416) which wraps all pallet events
console.log('\n\n=== GLOBAL RuntimeEvent STRUCTURE ===');
const runtimeEvent = lookup.find(x => x.id === 416);
if (runtimeEvent && runtimeEvent.type.def?.variant) {
  console.log('RuntimeEvent wraps these pallet variants:');
  runtimeEvent.type.def.variant.variants.forEach(v => {
    console.log(`  [${v.index}] ${v.name}`);
  });
}