# Apache Spark

## Clusters

O Apache Spark é usado para consumir, analisar e processar dados provenientes do data lake (normalmente).

### Partições

- O Apache Spark trabalha com os dados em memória, então eles precisam carregar esses dados de algum lugar, então I/O costuma ser uma dor de cabeça.
- Partições são "chunks", os dados são quebrados em fatias para então serem levados à memória.
- Default Partition: 64mb ou 128mb (Podem ser configuradas)
- Saber configurar partições permite criar processos eficientes.

### JOBS

- Aplicação em Spark, submetida ao cluster.
- **Driver**: recebe e compila as instruções, manda aos executores.
- **Executor**: executam as instruções.

### STAGES

- Jobs têm vários estágios, que permitem processar as informações. Gera planos que serão executados.

### TASKS

- Como as tarefas realmente vão ser executadas, em cima das partições.

### Transformações Estreitas (Narrow)

- As transformações estreitas são operações simples que ocorrem dentro de uma única partição dos dados.
- Elas não exigem a reorganização dos dados entre partições.
- Exemplos de transformações estreitas incluem `map`, `flatMap`, `filter`, `union`, `distinct`, `sample`, `sortBy`, `take`, `first`, `collect`, `foreach`, `mapPartitions` e `reduceByKey`.
- Essas transformações são mais rápidas, pois são executadas localmente em cada partição.

### Transformações Amplas (Wide)

- As transformações amplas envolvem operações que afetam todo o conjunto de dados, exigindo a reorganização dos dados entre diferentes partições.
- Elas geralmente estão relacionadas a agregações, junções ou reorganização de dados.
- Exemplos de transformações amplas incluem `groupByKey`, `aggregateByKey`, `join`, `cogroup`, `repartition`, `sortByKey`, `coalesce` e `glom`.
- Essas transformações podem ser mais complexas, pois envolvem troca de dados entre partições.

### RDD (Resilient Distributed Dataset)

- Abstração mais básica, a base para processamento de dados no Spark, uma coleção imutável de objetos distribuídos.
- São resilientes, podem ser reconstruídos em caso de falha.
- Distribuído em vários nós do cluster.
- Imutável, não pode ser alterado, mas transformável para criação de novos RDDs.

### DataFrame

- Coleção tipada em memória, uma tabela dividida em ranges colocada em memória.

### AQE (Adaptive Query Execution)

- Técnica utilizada para otimizar execução de consultas.
- Melhora a eficiência, adaptando-se dinamicamente ao ambiente de execução. Coleta dados estatísticos sobre os dados e o ambiente para tomar decisões de otimização.

