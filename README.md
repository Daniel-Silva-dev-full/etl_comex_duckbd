**ETL de Dados do Comércio Exterior com DuckDB e MinIO**
Este repositório contém um pipeline de ETL (Extract, Transform, Load) para processar dados do comércio exterior, utilizando DuckDB para processamento de dados, MinIO para armazenamento, e Metabase para visualização dos dados. O pipeline é dividido em três camadas: Raw, Bronze e Silver.

**Visão Geral**
O pipeline de ETL realiza as seguintes etapas:

**Extração:**
Dados são extraídos do bucket Raw no MinIO.

**Transformação:**
Os dados são carregados e transformados utilizando DuckDB.
As transformações incluem limpeza, filtragem e formatação de colunas.

**Carregamento:**
Os dados transformados são salvos em formato Parquet no bucket Bronze no MinIO.

A partir daí, são carregados e processados novamente para gerar dados finais no bucket Silver.

**Visualização:**
Os dados carregados no bucket Silver são usados para criar dashboards e relatórios interativos no Metabase.

**Alguns processos foram desenvolvidos de forma explícita para fins acadêmicos. Recomendo que, ao adaptar ou implementar esses processos, você siga as melhores práticas de desenvolvimento e segurança para garantir um sistema robusto e seguro.**
