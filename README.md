# MVP — Pipeline de Dados na Nuvem: IMDB Top 1000

**Aluno:** Gabriel Simas Gomes Barata  
**Plataforma:** Databricks Free Edition  
**Arquitetura:** Medalhão (Bronze → Silver → Gold) sobre Delta Lake  
**Notebook:** [`MVP.ipynb`](MVP.ipynb)  
**Repositório:** https://github.com/gabrielbarata/MVP-engenharia-de-dados  
---

## Contexto de Negócios e Perguntas (Etapas 2 e 4.1)

### Problema
Plataformas de streaming, produtoras e investidores precisam decidir **que tipo de filme produzir, comprar ou recomendar**. Errar essa decisão custa milhões. Este MVP constrói um pipeline que organiza os dados do IMDB Top 1000 para responder, com base em evidências, **o que caracteriza um filme bem avaliado**.

### Perguntas de negócio
1. **PG1** — Quais gêneros possuem a maior média de avaliação IMDB?
2. **PG2** — Filmes lançados a partir de 2000 têm avaliação média superior aos anteriores?
3. **PG3** — A duração do filme influencia a nota IMDB?
4. **PG4** — Existe correlação entre número de votos e nota IMDB?
5. **PG5** — Quais diretores (com ≥ 3 filmes no Top 1000) têm as melhores médias?

### Contexto dos dados brutos
- **Fonte:** [IMDB Dataset of Top 1000 Movies and TV Shows (Kaggle)](https://www.kaggle.com/datasets/harshitshankhdhar/imdb-dataset-of-top-1000-movies-and-tv-shows) — autor *Harshit Shankhdhar*.
- **Licença:** **CC0 — Public Domain**. Uso livre, inclusive comercial, sem necessidade de atribuição.
- **Estrutura bruta:** arquivo `imdb_top_1000.csv`, 1000 linhas × 16 colunas.
- **Colunas brutas:** `Poster_Link`, `Series_Title`, `Released_Year`, `Certificate`, `Runtime`, `Genre`, `IMDB_Rating`, `Overview`, `Meta_score`, `Director`, `Star1..Star4`, `No_of_Votes`, `Gross`.
- **Limitação reconhecida:** é um **recorte curado** (Top 1000), não uma amostra aleatória. Conclusões valem para filmes de elite, não para o mercado geral.

---

## Carga dos Dados (Etapa 4.2)

### Estratégia
Coleta via **`kagglehub`** (API oficial do Kaggle) executada direto no cluster Databricks. O CSV é lido com Pandas e convertido para Spark DataFrame **sem transformação semântica**.

### Metadados adicionados (rastreabilidade)
- `ingested_at` — timestamp da ingestão
- `source_file` — nome do arquivo de origem (`imdb_top_1000.csv`)

### Persistência
Tabela Delta `bronze.imdb_raw` no Unity Catalog. Schema `bronze` criado via SQL.

**Script:** ver notebook, seções **"2. Carga dos Dados — Camada Bronze"**.

**Evidência:** `docs/screenshots/02_bronze_table.png`.

---

## Modelagem e Catálogo de Dados (Etapa 4.3)

### Modelo adotado
Híbrido — **tabela fato + dimensão explodida + agregações por pergunta**:

```
bronze.imdb_raw               → dado bruto + metadados
silver.imdb_clean             → dado limpo, tipado, deduplicado
gold.fato_filmes              → fato com faixas derivadas (periodo, faixa_duracao)
gold.dim_genero               → dimensão de gênero (1 linha por filme-gênero)
gold.avg_rating_by_genre      → agregação PG1
gold.avg_rating_by_decade     → agregação PG2
gold.avg_rating_by_duration   → agregação PG3
gold.top_directors            → agregação PG5
gold.dq_summary               → resumo auditável de qualidade
```

### Por que `dim_genero` explodida?
O campo `Genre` é multivalorado (`"Crime, Drama"`). Sem explodir, um filme contaria 2× na média do gênero. Foi usado `explode(split(Genre, ","))`.

### Catálogo de Dados

#### `bronze.imdb_raw`
| Campo | Tipo | Descrição | Domínio |
|-------|------|-----------|---------|
| Poster_Link | string | URL do pôster | URL válida |
| Series_Title | string | Título | texto livre |
| Released_Year | string | Ano bruto | 4 dígitos |
| Certificate | string | Classificação | G, PG, PG-13, R, U, A, UA |
| Runtime | string | Duração bruta ("142 min") | texto |
| Genre | string | Gêneros separados por vírgula | Drama, Crime… |
| IMDB_Rating | double | Nota IMDB | 0–10 |
| Overview | string | Sinopse | texto livre |
| Meta_score | double | Metacritic | 0–100 ou nulo |
| Director | string | Diretor | texto livre |
| Star1..Star4 | string | Elenco principal | texto livre |
| No_of_Votes | long | Nº de votos | > 0 |
| Gross | string | Bilheteria bruta | texto |
| ingested_at | timestamp | Data/hora da ingestão | ISO 8601 |
| source_file | string | Arquivo de origem | `imdb_top_1000.csv` |

#### `silver.imdb_clean`
Mesmas colunas da Bronze, com ajustes de tipo + nova coluna:
- `Released_Year` → **int**
- `Runtime_min` → **int** (nova, derivada de `Runtime`)
- `Meta_score` → **int**
- `Gross` → **double**
- `Certificate`, `Genre`, `Director` → `trim()` aplicado

#### `gold.fato_filmes`
| Campo | Tipo | Descrição |
|-------|------|-----------|
| Series_Title | string | Título |
| Director | string | Diretor |
| Certificate | string | Classificação |
| Released_Year | int | Ano |
| Runtime_min | int | Duração (min) |
| Genre | string | Gêneros multivalorados |
| IMDB_Rating | float | Nota IMDB |
| Meta_score | int | Metacritic |
| No_of_Votes | long | Nº de votos |
| Gross | double | Bilheteria (USD) |
| periodo | string | `"2000+"` / `"Antes de 2000"` |
| faixa_duracao | string | `"Curto"` / `"Médio"` / `"Longo"` |

#### `gold.dim_genero`
| Campo | Tipo | Descrição |
|-------|------|-----------|
| Series_Title | string | FK para `fato_filmes` |
| IMDB_Rating | float | Nota (desnormalizada) |
| Released_Year | int | Ano |
| genero | string | Gênero individual (pós-explode) |

#### Agregações Gold (`avg_rating_by_genre`, `avg_rating_by_decade`, `avg_rating_by_duration`, `top_directors`)
Colunas padrão: `chave` (string), `media_nota` (double), `qtd_filmes` (long). Algumas incluem `media_votos`.

#### `gold.dq_summary`
| Campo | Tipo | Descrição |
|-------|------|-----------|
| metrica | string | Nome do indicador |
| valor | double | Valor medido |
| detalhe | string | Contexto do indicador |

### Linhagem
```
Kaggle CSV → bronze.imdb_raw → silver.imdb_clean → gold.fato_filmes / gold.dim_genero → agregações gold.*
```
Rastreada automaticamente pelo **Unity Catalog** (aba *Lineage*).

**Evidências:** `docs/screenshots/01_catalog.png`, `05_lineage.png`, `11_dq_summary.png`.

---

## Pipeline de Dados (Etapa 4.4)

### Organização
Tudo em **um único notebook** (`MVP.ipynb`), dividido em seções — uma por camada — para manter linearidade e reprodutibilidade. Cada seção é independente o suficiente para ser reexecutada sem afetar as outras.

### Fluxo ETL
1. **Extract** — `kagglehub` baixa o CSV.
2. **Transform (Bronze → Silver)** — PySpark: cast de tipos, `regexp_extract` para `Runtime_min`, `regexp_replace` para `Gross`, `trim` em strings, `dropDuplicates`, filtro de `Released_Year` e `IMDB_Rating` nulos.
3. **Transform (Silver → Gold)** — SQL no Databricks: `CASE WHEN` para `periodo` e `faixa_duracao`, `explode` para gênero, agregações `AVG`/`COUNT`/`CORR`.
4. **Load** — `.saveAsTable(...)` em Delta Lake nas 3 camadas.

### Documentação das transformações
Todas as transformações estão comentadas **célula a célula** no notebook, com o motivo de cada uma (ex.: *"explode do Genre para evitar dupla contagem em médias"*).

**Evidências:** `docs/screenshots/02_bronze_table.png`, `03_silver_table.png`, `04_gold_tables.png`.

---

## Qualidade de Dados (Etapa 4.5)

### Metodologia
Verificação de 6 dimensões clássicas sobre `silver.imdb_clean`.

| Dimensão | Verificação | Resultado |
|---------|-------------|-----------|
| **Completude** | Contagem de nulos por coluna | `Certificate`: 101, `Meta_score`: 157, `Gross`: 169. Demais: 0. |
| **Unicidade** | `count()` vs `distinct().count()` | 999 / 999 → **0 duplicatas** |
| **Consistência** | Domínio de `Certificate` e combinações de `Genre` | Padrões estáveis (U, A, UA, R…); gêneros multivalorados controlados |
| **Acurácia** | Notas fora de [0,10]; anos fora de [1900,2025]; durações fora de [1,1000] | **0 ocorrências** |
| **Domínio** | Estatísticas descritivas de colunas numéricas | `IMDB_Rating` ∈ [7.6, 9.3]; `Released_Year` ∈ [1920, 2020] |
| **Outliers** | Regra 2σ (bilateral) + IQR (1.5×) | 2σ: 33 sup. / 0 inf. IQR: 13 sup. / 0 inf. **Nenhum outlier inferior** → confirma dataset curado |

### Problemas detectados e como foram tratados
| Problema | Tratamento aplicado |
|----------|---------------------|
| `Released_Year` como string | `rlike("^[0-9]{4}$")` + `cast("int")` |
| `Runtime` como `"142 min"` | `regexp_extract(col, r"(\d+)")` → `Runtime_min` |
| `Gross` como `"28,341,469"` | `regexp_replace(",", "")` + `cast("double")` |
| `Genre` multivalorado | `explode(split(Genre, ","))` em `gold.dim_genero` |
| Nulos em `Certificate`/`Meta_score`/`Gross` | **Mantidos**, pois a ausência é informativa (não inventar dado) |
| Linhas sem `Released_Year`/`IMDB_Rating` | Filtradas (999 de 1000) |

### Evidência
`docs/screenshots/11_dq_summary.png` — tabela `gold.dq_summary` persistida.

---

## Análise de Dados (Etapa 4.5)

### PG1 — Gêneros com maior média
| Gênero | Média | Nº filmes |
|--------|-------|-----------|
| War | **8.014** | 51 |
| Western | 8.000 | 20 |
| Film-Noir | 7.989 | 19 |
| Sci-Fi | 7.978 | 67 |
| Drama | 7.960 | **723** |

**Discussão:** gêneros de nicho lideram, mas com volume pequeno (**survivorship bias** — só o topo entra). Drama mantém média competitiva com 723 filmes (14× a amostra de War) → **robusto e comercialmente recomendado**.

*Evidência:* `docs/screenshots/06_pg1_genre.png`.

### PG2 — Filmes pós-2000 são melhores?
| Período | Média | Nº |
|---------|-------|----|
| Antes de 2000 | **7.982** | 514 |
| 2000+ | 7.915 | 485 |

**Discussão:** diferença pequena (+0.067 a favor dos pré-2000). Era **não é preditor forte**. Pré-2000 tem vantagem porque passou pelo teste do tempo.

*Evidência:* `docs/screenshots/07_pg2_periodo.png`.

### PG3 — Duração influencia a nota?
| Faixa | Média nota | Média votos | Nº |
|-------|-----------|-------------|----|
| Longo (>150min) | **8.097** | 362.546 | 147 |
| Médio (100–150min) | 7.928 | 281.390 | 667 |
| Curto (<100min) | 7.910 | 175.363 | 185 |

**Discussão:** longos têm média **+0.17** maior. Correlação com ambição artística e engajamento. **Não é "ser longo" que gera nota**, é o tipo de filme (épicos, dramas históricos). *Sweet spot* comercial continua 100–150min.

*Evidência:* `docs/screenshots/08_pg3_duracao.png`.

### PG4 — Correlação votos × nota
**Pearson r = 0.4954** (moderada positiva). r² ≈ 0.245 → 25% da variação da nota é explicada pelos votos.

**Discussão:** popularidade e qualidade **andam juntas mas não perfeitamente**. Existem **joias escondidas** (alta nota, baixo volume) — boas para curadoria editorial. Marketing sozinho não garante nota.

*Evidência:* `docs/screenshots/09_pg4_correlacao.png`.

### PG5 — Melhores diretores (≥3 filmes)
| Diretor | Média | Nº |
|---------|-------|----|
| Christopher Nolan | **8.462** | 8 |
| Peter Jackson | 8.400 | 5 |
| Francis Ford Coppola | 8.400 | 5 |
| Charles Chaplin | 8.333 | 6 |
| Sergio Leone | 8.267 | 6 |
| Stanley Kubrick | 8.233 | 9 |
| Akira Kurosawa | 8.220 | 10 |

**Discussão:** Nolan lidera em média **e** volume. Kubrick e Kurosawa mantêm >8.2 com 9–10 filmes — **consistência ainda mais difícil**. Para aquisição/contratação, esses nomes reduzem risco percebido.

*Evidência:* `docs/screenshots/10_pg5_diretores.png`.

### Conclusão geral — o que caracteriza um filme bem avaliado?
1. **Gênero:** Drama (ou Crime+Drama); nichos (War, Film-Noir) para prestígio.
2. **Duração:** >150 min (épicos e dramas históricos).
3. **Popularidade:** alta (r ≈ 0.50), com espaço para joias de nicho.
4. **Direção:** histórico consistente (Nolan, Kubrick, Kurosawa).
5. **Era:** fator secundário.

**Aplicação:** priorizar **Drama de longa duração com diretor comprovado**; usar popularidade como sinal parcial; explorar nichos para diferenciar catálogo.

---

## Autoavaliação

### Objetivos atingidos
✅ As 5 perguntas foram respondidas com dados persistidos nas 3 camadas.  
✅ Pipeline reprodutível de ponta a ponta em um único notebook.  
✅ Catálogo de dados completo documentado (README + Unity Catalog).

### Dificuldades encontradas
1. **Tipagem de `Released_Year`** — pandas leu como string; solução: `rlike` + `cast`.
2. **`Genre` multivalorado** — distorcia médias; solução: `dim_genero` explodida.
3. **Performance no Free Edition** — agregações feitas em SQL (Spark) e não em pandas.
4. **Outliers** — reforçada análise bilateral (2σ + IQR), confirmando que só há outliers superiores (dataset curado).

### Trabalhos futuros
- Ingestão incremental (Auto Loader).
- Segunda fonte (bilheteria ajustada por inflação, séries de TV).
- Dashboard no Databricks SQL.
- Testes automatizados (DLT Expectations / Great Expectations).
- Publicar Gold como *Data Products* no Unity Catalog.

### Aprendizado principal
**Engenharia de dados começa pelo "porquê".** Sem perguntas claras, o pipeline vira exercício técnico vazio.

---

## Como reproduzir
1. Abrir o notebook `MVP.ipynb` no Databricks Free Edition.
2. Rodar as células na ordem.
3. Verificar as tabelas criadas nos schemas `bronze`, `silver`, `gold`.