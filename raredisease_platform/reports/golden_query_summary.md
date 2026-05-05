# Golden Query Evaluation Summary

Passed: **7/7**

| Query ID | Pass | Entities | Entity types | Literature results | Missing labels | Missing types |
|---|---:|---|---|---:|---|---|
| fop_acvr1_case_reports | yes | acvr1, fibrodysplasia ossificans progressiva | disease, gene | 9 | - | - |
| gene_only_acvr1 | yes | acvr1 | gene | 10 | - | - |
| hgnc_id_acvr1 | yes | acvr1 | gene | 10 | - | - |
| ensembl_acvr1 | yes | acvr1 | gene | 10 | - | - |
| fop_acvr1_ambiguous_abbreviation | yes | acvr1, cep43, chtop | gene | 9 | - | - |
| heterotopic_ossification_acvr1 | yes | acvr1, ectopic ossification | gene, phenotype | 10 | - | - |
| compound_gene_aspirin_acvr1 | yes | acvr1, aspirin | compound, gene | 2 | - | - |

## Debug details

### fop_acvr1_case_reports

- Pass: yes
- Pipeline steps: normalize_entities, search_literature, search_structured_evidence, assemble_evidence_graph
- Detected candidates: ACVR1 [gene]; fibrodysplasia ossificans progressiva [disease]
- Connector calls: ACVR1->hgnc:ok/1; fibrodysplasia ossificans progressiva->orphadata:ok/5
- Warnings: -

### gene_only_acvr1

- Pass: yes
- Pipeline steps: normalize_entities, search_literature, search_structured_evidence, assemble_evidence_graph
- Detected candidates: ACVR1 [gene]
- Connector calls: ACVR1->hgnc:ok/1
- Warnings: -

### hgnc_id_acvr1

- Pass: yes
- Pipeline steps: normalize_entities, search_literature, search_structured_evidence, assemble_evidence_graph
- Detected candidates: HGNC:171 [gene]
- Connector calls: HGNC:171->hgnc:ok/1
- Warnings: -

### ensembl_acvr1

- Pass: yes
- Pipeline steps: normalize_entities, search_literature, search_structured_evidence, assemble_evidence_graph
- Detected candidates: ENSG00000115170 [gene]
- Connector calls: ENSG00000115170->hgnc:ok/1
- Warnings: -

### fop_acvr1_ambiguous_abbreviation

- Pass: yes
- Pipeline steps: normalize_entities, search_literature, search_structured_evidence, assemble_evidence_graph
- Detected candidates: FOP [gene]; ACVR1 [gene]
- Connector calls: FOP->hgnc:ok/3; ACVR1->hgnc:ok/1
- Warnings: -

### heterotopic_ossification_acvr1

- Pass: yes
- Pipeline steps: normalize_entities, search_literature, search_structured_evidence, assemble_evidence_graph
- Detected candidates: ACVR1 [gene]; heterotopic ossification [phenotype]
- Connector calls: ACVR1->hgnc:ok/1; heterotopic ossification->hpo:ok/2
- Warnings: -

### compound_gene_aspirin_acvr1

- Pass: yes
- Pipeline steps: normalize_entities, search_literature, search_structured_evidence, assemble_evidence_graph
- Detected candidates: ACVR1 [gene]; aspirin [compound]
- Connector calls: ACVR1->hgnc:ok/1; aspirin->pubchem:ok/1
- Warnings: -

## Notes

- Failures should be inspected before changing agent behavior.
- Ambiguous acronym queries may pass if the gene is resolved and ambiguity is exposed through alternatives/trace.
- Literature result counts can vary because live biomedical APIs change over time.