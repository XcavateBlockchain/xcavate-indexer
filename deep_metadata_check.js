const fs = require('fs');
const meta = JSON.parse(fs.readFileSync('metadata_new.json', 'utf8'));
const v15 = meta.metadata.v15 || {};

// In v15, events might be in the type section or as a top-level type
console.log('=== v15 top-level keys ===');
console.log(Object.keys(v15));

// Check the "type" field
if (v15.type) {
  console.log('\n=== v15.type ===');
  console.log(JSON.stringify(v15.type, null, 2).substring(0, 3000));
}

// Check extrinsic structure
if (v15.extrinsic) {
  console.log('\n=== v15.extrinsic ===');
  console.log(JSON.stringify(v15.extrinsic, null, 2).substring(0, 2000));
}

// Check all lookup types for anything with "Event" in the name
console.log('\n=== LOOKUP TYPES CONTAINING "Event" ===');
if (v15.lookup?.types) {
  v15.lookup.types.forEach(t => {
    const path = t.type.path?.join('::') || '';
    if (path.toLowerCase().includes('event')) {
      console.log(`  Type[${t.id}]: ${path}`);
    }
    // Also check composite fields for Event types
    if (t.type.def?.composite?.fields) {
      t.type.def.composite.fields.forEach(f => {
        const fpath = t.type.path?.join('::') || '';
        if (path.toLowerCase().includes('event') || (f.type && f.name && f.name.toLowerCase().includes('event'))) {
          console.log(`  Type[${t.id}].fields.${f.name}: path=${path}, field=${f.name}`);
        }
      });
    }
  });
}

// Check outerEnums - events might be there
if (v15.outerEnums) {
  console.log('\n=== v15.outerEnums ===');
  console.log(JSON.stringify(v15.outerEnums, null, 2).substring(0, 5000));
}

// Check apis
if (v15.apis) {
  console.log('\n=== v15.apis ===');
  console.log(JSON.stringify(v15.apis, null, 2).substring(0, 3000));
}

// Check custom
if (v15.custom) {
  console.log('\n=== v15.custom ===');
  console.log(JSON.stringify(v15.custom, null, 2).substring(0, 3000));
}

// Print all pallet names for context
console.log('\n=== ALL 29 PALLiets ===');
v15.pallets.forEach((p, i) => console.log(`  [${i}] ${p.name}`));