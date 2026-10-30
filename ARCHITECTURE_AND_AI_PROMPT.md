# Byteflow I/O — razão arquitetural e instruções de integração

## Parte 1 — Por que criar outra crate

A proposta é manter duas crates com uma dependência em uma única direção:

```text
byteflow-io  ───────────────►  byteflow
integração do host              runtime/core
```

O `byteflow` deve continuar sendo o núcleo de concorrência embutível. Sua responsabilidade é executar bytecode, escalonar flows, controlar mailboxes, aplicar quotas e capabilities, entregar mensagens Atomic Hop e gerenciar supervisão. Ele não deve conhecer MQTT, UART, GPIO, ESP32, gateways de pagamento ou SDKs de fornecedores.

O `byteflow-io` representa a fronteira entre esse runtime e o mundo externo. Ele contém adaptadores e contratos para que o processo hospedeiro conecte drivers de hardware, brokers e serviços externos sem aumentar a ISA nem acoplar o runtime a bibliotecas específicas.

Essa separação tem motivos concretos:

1. **Dependências e portabilidade:** quem usa Byteflow apenas como runtime não precisa compilar ou carregar bibliotecas de MQTT, serial, GPIO ou pagamento.
2. **Estabilidade do core:** mudanças de SDKs e protocolos externos não devem forçar releases, mudanças de ABI ou alterações no bytecode.
3. **Segurança:** o host decide quais drivers existem e quais flows recebem capabilities para chamar cada native. `NativeMask` continua sendo política do core, não uma permissão implícita concedida por `byteflow-io`.
4. **Concorrência previsível:** natives síncronas fazem somente validação e enqueue rápido. Operações demoradas ficam em workers do host e retornam por `HostAwaitCompleter`.
5. **Reutilização:** uma mesma versão de `byteflow-io` pode oferecer integração para diferentes aplicações, enquanto drivers concretos podem variar por sistema operacional, hardware ou fornecedor.

O `byteflow-io` não é um segundo runtime, não possui uma VM própria, não cria flows próprios e não deve duplicar o scheduler M:N do Byteflow.

## Parte 2 — Mapeamento dos mecanismos

| Mecanismo do Byteflow | Papel na integração |
|---|---|
| `CallNative` / `CallNativeFrom` | Chamar operações host-side rápidas, como validar e colocar um comando em fila limitada |
| `HostAwait` / `HostAwaitBridge` | Executar operações demoradas, como MQTT com ACK, consulta de pagamento e espera de ACK da ESP |
| `Runtime::send` | Entregar eventos externos a um Flow como `Value::Message` |
| `OutputSink` / `print` | Log e diagnóstico; nunca usar como canal de controle |
| `SendRemote` / transport | Somente quando o gateway/peer está em outro processo; não é requisito para I/O local |

## Parte 3 — Contratos obrigatórios

- Nenhum opcode novo de I/O.
- A VM não abre sockets, arquivos, UART ou GPIO.
- `CallNative` nunca espera por rede, broker, dispositivo, pagamento ou mutex de longa duração.
- O `try_submit` do driver deve ser realmente não bloqueante e usar fila limitada.
- O retorno da native rápida significa apenas “comando aceito pela fila do host”, não “ação física concluída”.
- O resultado final de uma operação lenta deve voltar por `HostAwaitCompleter::complete` ou `fail`.
- Workers de HostAwait devem ter limite de quantidade e de fila, deadlines e cancelamento cooperativo.
- Saturação de fila deve falhar explicitamente; nunca criar threads ilimitadas como fallback.
- `Runtime::send` só recebe `Value::Message`; o helper `EventRouter` encapsula eventos em `Message::request`.
- Índices padrão `0..=7` continuam estáveis. Natives de integração começam em `8` e são append-only.
- O acesso a natives continua condicionado a `CapRights::NATIVE` e `NativeMask` no Byteflow.
- Não usar `unwrap`/`expect` em mutexes no código de produção.
- Shutdown: bloquear novas entradas externas, chamar `Runtime::shutdown`, drenar/juntar workers de I/O e só então encerrar os drivers/dispositivos.

## Parte 4 — Prompt para a outra IA

Copie o texto a seguir junto com a pasta `byteflow-io` e o repositório Byteflow.

---

# Tarefa: integrar a crate `byteflow-io` ao workspace Byteflow

Você está trabalhando no repositório real `mchael158/bytecode-vm`. Sua tarefa é integrar e validar uma segunda crate chamada `byteflow-io`, preservando rigorosamente a arquitetura e os contratos existentes do Byteflow.

## Objetivo

Adicionar `byteflow-io` como crate independente de integração do host, com dependência unidirecional:

```text
byteflow-io -> byteflow
```

O pacote `byteflow-actors` continua sendo o core e a crate Rust importável continua se chamando `byteflow`. O pacote novo se chama `byteflow-io` e sua lib se chama `byteflow_io`.

## Primeiro: faça uma inspeção real do repositório

Antes de alterar qualquer coisa:

1. Leia o `Cargo.toml` raiz, o manifesto de `crates/byteflow`, os módulos públicos, `docs/host-await.md`, `docs/production-embedding.md`, `docs/atomic-hop.md`, `docs/security.md`, `src/vm/native.rs`, `src/natives.rs`, `src/scheduler/host_await.rs` e os exemplos existentes.
2. Confirme a versão real do pacote no workspace e a versão de API efetivamente presente no código local. Não presuma que docs.rs, crates.io, este prompt ou comentários estejam sincronizados com `HEAD`.
3. Compare cada import, tipo, campo, assinatura e método do código fornecido em `byteflow-io/` com o código local. Corrija diferenças somente na camada de integração, salvo prova de bug real no core.
4. Não invente APIs, campos, opcodes, constantes, variantes ou construtores. Não substitua métodos reais por pseudocódigo.
5. Identifique se o workspace usa `resolver = "2"`, `rust-version`, lint settings e convenções próprias; preserve essas decisões.

## Motivo de manter duas crates

O `byteflow` precisa permanecer independente de protocolos, SDKs e hardware. Ele administra VM, bytecode, scheduler M:N, flows, mailboxes, Atomic Hop, capabilities, quotas e supervisão. O host é dono de sockets, arquivos, GPIO, UART, MQTT, pagamentos e comunicação com ESP.

O `byteflow-io` é somente a camada de integração entre o runtime e esses drivers. Ele não deve se tornar outro scheduler, outro runtime, uma VM paralela ou uma camada de I/O dentro do bytecode.

A dependência deve ser unilateral: `byteflow-io` depende de `byteflow`; `byteflow` nunca depende de `byteflow-io`.

## Implementação esperada

Integre os arquivos fornecidos e adapte-os às APIs locais. Preserve as responsabilidades abaixo:

### A. Fast natives

- `CallNative` / `CallNativeFrom` são executadas inline no worker do Byteflow.
- Natives rápidas devem somente validar os argumentos e chamar `IoDriver::try_submit`.
- `try_submit` coloca o comando em fila limitada e retorna um ID sem esperar o resultado do dispositivo ou da rede.
- MQTT publish com espera de ACK, pagamento e confirmação da ESP não podem executar dentro de uma native síncrona.
- Preserve os slots padrão `0..=7` do Byteflow. As natives desse pacote começam no slot `8`.
- Não renumerar nomes ou índices padrão. Se a API de `NativeTable` exigir composição manual, reutilize os `NativeFn` existentes da tabela padrão; não construa uma tabela vazia que deixe os slots padrão sem implementação.

### B. HostAwait

- Implementar `HostAwaitBridge` com uma fila limitada e um número configurável de workers dedicados ao I/O do host.
- `submit` deve retornar prontamente.
- Encaminhar operações demoradas para workers fora do scheduler do Byteflow.
- Completar com `HostAwaitCompleter::complete` ou falhar com `fail`.
- Respeitar `is_cancelled` e o método `cancel` do bridge.
- Não criar thread por requisição sem limite.
- Na saturação, falhar explicitamente e não bloquear o worker do Byteflow.
- Aplicar deadlines finitos nos drivers concretos. Cancelamento cooperativo não substitui timeout.
- Proteger fronteiras de driver contra panic e não deixar um panic derrubar os workers restantes.

### C. Entrada de eventos

- Callbacks MQTT/UART/GPIO podem ser executados fora do runtime.
- Injetar eventos no Flow alvo com `Runtime::send`.
- Sempre enviar `Value::Message`; nunca mandar `Value::Int`, `Pid` ou `Str` diretamente.
- Usar `Message::request` ou o construtor equivalente existente no código local.
- Respeitar os tipos `FlowId`, `Message`, `SendError` e os mecanismos de autenticação do scheduler.
- Em ambientes multi-tenant, não aceitar `FlowId` fornecido por bytecode não confiável: usar capabilities/registro controlado pelo host.

### D. Observabilidade

- Usar `OutputSink` / `print` para diagnóstico write-only.
- Não usar log como canal de controle ou transporte de eventos.
- Não adicionar dependência de logging se `OutputSink` do core já atende.

### E. Drivers concretos

`IoDriver` é a fronteira do host. Implemente adapters reais somente se o repositório/projeto fornecer os requisitos concretos:

- MQTT: biblioteca escolhida, broker, autenticação, QoS, política de reconnect e ACK.
- UART: sistema operacional, porta, baud rate, framing e timeout.
- GPIO: biblioteca/driver do SO, número/identificação de chip e política de ownership.
- Pagamentos: provedor, ambiente sandbox/produção, autenticação, idempotência e timeout.
- ESP32: protocolo, tópicos/frames, identificação, ACK/retry e firmware.

Não invente credenciais, endpoints ou SDKs. Enquanto esses dados não existirem, entregue adapters por trait e fakes determinísticos para testes; declare claramente o que é interface e o que é integração física real.

### F. Opcional: gateway remoto

Não implemente TCP/TLS ou `SendRemote` por padrão para um único processo gateway. Só adicione transport quando houver requisito de outro processo. O host continua dono do socket, TLS, autenticação e roteamento; não mova socket para a VM.

## Manifesto e workspace

- Adicione `crates/byteflow-io` aos `members` do workspace sem remover membros existentes.
- Configure o pacote `byteflow-io` para depender do crate local `byteflow-actors` por `path`, usando a localização real confirmada no workspace.
- Não crie dependência circular.
- Não altere o nome do pacote core nem o nome da lib importada.
- Não force dependências MQTT/UART/GPIO/payment no `default` feature.
- Se forem criados adapters com SDKs concretos, use features opcionais e explique o custo/dependências.

## Testes obrigatórios

Crie/execute testes que provem:

1. Os natives padrão `0..=7` continuam presentes e com os mesmos nomes/índices.
2. As natives do `byteflow-io` ocupam somente slots `>= 8`.
3. Native rápida faz enqueue e retorna sem aguardar a execução do dispositivo.
4. QoS inválido, argumentos faltantes, tipos errados e IDs fora da faixa falham como `Fault` sem panic.
5. Fila de HostAwait cheia falha de forma explícita e não bloqueia worker do Byteflow.
6. Um HostAwait bem-sucedido conclui uma operação de teste e devolve a resposta ao Flow.
7. Falha do driver vira falha de HostAwait, não deadlock.
8. Cancelamento/shutdown de HostAwait não deixa o Flow preso nem o worker em loop infinito.
9. `Runtime::send` injeta evento `Value::Message` e a recepção seletiva correta desbloqueia o Flow.
10. `cargo test` cobre build sem adapters opcionais e com cada feature disponível.

Não escreva um teste que dependa de broker, ESP físico, UART real ou gateway de pagamento em CI padrão. Use fakes; deixe hardware-in-the-loop como teste separado e documentado.

## Qualidade e verificação

Execute no workspace real, corrigindo os problemas:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Se o projeto tiver CI com JIT e sem JIT, rode os dois modos. Não desligue lint, não reduza testes existentes e não altere comportamento do core para mascarar erros de integração.

## Restrições não negociáveis

- Não adicionar opcode de I/O.
- Não alterar o formato de `.bf` nem ABI por causa de `byteflow-io`.
- Não colocar MQTT/TCP/UART/GPIO/filesystem/payment dentro de `byteflow`.
- Não bloquear workers em natives.
- Não enviar valores crus por `Runtime::send`.
- Não renumerar natives padrão.
- Não conceder `CapRights::NATIVE` implicitamente a flows confinados.
- Não usar `unwrap`/`expect` em mutexes no código de produção.
- Não declarar suporte físico a um hardware/provedor se somente existe trait ou mock.

## Entrega final

Ao terminar, apresente:

1. Árvore final de arquivos.
2. Diff dos manifests/workspace.
3. Descrição de cada módulo do `byteflow-io`.
4. APIs reais do core usadas e versão confirmada.
5. Índices nativos finais.
6. Operações HostAwait e respectivos IDs.
7. Testes executados e resultados reais.
8. Limitações ainda existentes, especialmente drivers que dependem de hardware/SDK/credenciais.
9. Confirmação explícita de que `byteflow` não recebeu dependência de `byteflow-io` e não ganhou I/O na VM.

Não afirme que testes passaram se não foram executados. Não faça commits ou push sem solicitação explícita.

---

## Nota de versão desta entrega

O código desta pasta está alinhado com a API do workspace local `byteflow-actors` **0.9.21** (`Desktop/Bytecode/crates/byteflow`). A IA que integrar no repositório deve tratar o código local real como fonte de verdade, comparar assinaturas e então ajustar a dependência `path`. Não faça downgrade do core apenas para acomodar esse código.
