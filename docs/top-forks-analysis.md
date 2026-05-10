# Top Forks Analysis

基线：`hank9999/kiro.rs` 的 `origin/master`，本地基线提交 `19aea59d77a01e035513d968616f5d532158ded1`。

统计方法：通过 GitHub fork 列表获取活跃 fork，fetch 到本地 `refs/remotes/forks/*` 后，用 `git rev-list --count origin/master..<fork_ref>` 计算 ahead commits。更新时间：2026-05-11。

| # | Fork | Branch | Ahead commits | 本地 ref | 初步功能摘要 | 建议 |
|---:|---|---|---:|---|---|---|
| 1 | `BenedictKing/kiro.rs` | `master` | 270 | `refs/remotes/forks/BenedictKing_kiro.rs/master` | 综合增强：prompt cache usage、压缩、冷却/限流、用户亲和、CLI/IDE endpoint、Admin 配置 | 优先拆后端核心，不直接 merge |
| 2 | `gitmzc/kiro.rs` | `master` | 115 | `refs/remotes/forks/gitmzc_kiro.rs/master` | SQLite 请求统计、日志流、`/cc/v1` 缓冲流、会话粘滞、WebSearch、管理端重构 | 统计/日志二阶段参考 |
| 3 | `easayliu/kiro.rs` | `master` | 80 | `refs/remotes/forks/easayliu_kiro.rs/master` | 更精细 prompt cache 模拟、cache 分桶、用户绑定、429 退避、Admin UI 改进 | cache tracker 规则优先吸收 |
| 4 | `hcscq/kiro.rs` | `master` | 75 | `refs/remotes/forks/hcscq_kiro.rs/master` | 并发上限、token bucket、模型策略、请求权重、Redis 共享运行态、WebFetch | 强但复杂，适合后续专项 |
| 5 | `luluxiuxiu/kiro.rs` | `master` | 61 | `refs/remotes/forks/luluxiuxiu_kiro.rs/master` | 轻量统计、历史截断、流重试、按凭据模型、余额增强 | 轻量 StatsStore 可先参考 |
| 6 | `BaSui01/kiro.rs` | `master` | 53 | `refs/remotes/forks/BaSui01_kiro.rs/master` | API key 管理、凭据池、池级路由、CSRF、历史管理、CLI | API key/池路由后置 |
| 7 | `Cen-Yaozu/kiro.rs` | `master` | 44 | `refs/remotes/forks/Cen-Yaozu_kiro.rs/master` | least-connections、token 诊断、友好错误、Bonus 额度 | 单点功能可摘取 |
| 8 | `coderdkai/kiro.rs` | `master` | 38 | `refs/remotes/forks/coderdkai_kiro.rs/master` | SQLite 持久化、Device Flow、自动注册、web session 导出 | 产品化登录/存储时评估 |
| 9 | `Theo-jobs/kiro.rs` | `master` | 29 | `refs/remotes/forks/Theo-jobs_kiro.rs/master` | Redis 缓存热更新、企业 OIDC、全局/凭据代理、临时禁用恢复 | 只参考可选策略 |
| 10 | `siyuan-123/kiro.rs` | `openai-compat` | 26 | `refs/remotes/forks/siyuan-123_kiro.rs/openai-compat` | OpenAI Chat/Responses 兼容、动态模型、代理池、活动监控 | OpenAI 兼容可独立移植 |

## 当前集成优先级

1. Prompt cache usage 模拟：参考 `easayliu` / `BenedictKing`，当前集成分支已接入轻量版。
2. 限流冷却和用户亲和：参考 `BenedictKing` / `easayliu`，当前集成分支已接入短冷却、成功恢复、conversation 亲和。
3. 调用记录：参考 `luluxiuxiu` 的轻量统计方向，当前集成分支已先接入 JSONL 调用记录；暂不引入 SQLite 请求流水。
4. 上游错误安全映射和大体量日志截断：参考 `hcscq` / `Cen-Yaozu`，当前集成分支已接入安全错误映射、UTF-8 安全截断、上游错误体截断。
5. OpenAI 兼容：参考 `siyuan-123`，建议单独分支/批次移植。
6. `hcscq` 的 token bucket/并发/Redis 共享态能力强，但不适合和第一批功能混合落地。

## 重复功能方案对比

| 功能主题 | 候选实现 | 方案评估 | 当前决策 |
|---|---|---|---|
| Prompt cache usage 模拟 | `easayliu`、`BenedictKing` | `easayliu` 的实现更贴近 Claude Code 真实请求形态：按 `metadata.user_id` 分桶、剥离 billing header、20 段回扫、5m/1h TTL 分桶和 cache skip rate 都更完整；`BenedictKing` 覆盖面广，测试多，但部分实现较早期，lookback 默认更保守。 | 采用混合方案：以 `easayliu` 的 cache tracker 规则为主，吸收 `BenedictKing` 的 usage 注入覆盖面。当前分支已接入核心规则。 |
| 凭据冷却/限流恢复 | `BenedictKing`、`easayliu`、`hcscq` | `BenedictKing` 的全凭据冷却快速返回和 `Retry-After` 语义最好；`easayliu` 的退避节奏简单；`hcscq` 的 token bucket/队列/并发策略最强，但会显著改变请求分布。 | 当前先采用轻量冷却：429/408/5xx 临时冷却、成功恢复、全部冷却时快速失败。后续优先补 `Retry-After`，暂缓 token bucket/队列。 |
| 账号亲和/粘滞 | `BenedictKing`、`easayliu`、`gitmzc` | `BenedictKing`/`easayliu` 使用和 cache identity 相关的 binding key，能减少跨账号反复预热；`gitmzc` 的 session 粘滞与统计系统耦合更重。 | 采用轻量 conversation affinity，不引入完整 session 存储。后续如做 SQLite 统计，再评估 session 级持久粘滞。 |
| 调用记录/统计 | `luluxiuxiu`、`gitmzc`、`BenedictKing` | `luluxiuxiu` 的轻量统计方向适合第一批；`gitmzc` 的 SQLite 请求流水和前端日志流可观测性更强但改动大；`BenedictKing` 的 sensitive logs/截断思路更重视隐私边界。 | 当前采用 JSONL 轻量记录，并做脱敏和 UTF-8 安全截断。后续应补“默认关闭或显式 sensitive logging 开关”的策略，再考虑 SQLite。 |
| 上游错误映射 | `hcscq`、`BenedictKing`、`Cen-Yaozu` | `hcscq` 的 `PublicProviderError` 类型化错误最好，能避免字符串匹配脆弱；`BenedictKing` 的 400 诊断和大 body 截断也有价值；`Cen-Yaozu` 偏友好提示和 token 诊断。 | 当前先用保守字符串映射和安全截断。后续建议独立小补丁引入 typed provider error，再替换字符串匹配。 |
| Stream 稳定性/重试 | `hcscq`、`luluxiuxiu`、`gitmzc` | `hcscq` 的“发送 SSE 前 bounded retry”风险较低；`luluxiuxiu` 的 stream interruption retry 和 history truncation 功能强，但可能改变 Claude Code 工具调用连续性；`gitmzc` 的 `/cc/v1` 缓冲流更偏协议兼容。 | 暂缓。Claude Code 已经出现过中断问题，stream retry/history truncation 需要真实压测后单独接入。 |
| 真响应缓存 response replay | `Jarvisu88` 非 top10 fork，提交 `802eacd` 后又被 `c9c7270` revert | 方案有价值：只缓存完全相同的非流式请求、跳过 stream 和 WebSearch、TTL 文件缓存、命中跳过上游。但它后来被作者撤回，且缓存完整响应可能重放旧 tool_use/id/usage，对 Claude Code 长会话和工具链有中等风险。 | 暂不接入主链路。若要做，只能 opt-in、默认关闭、限制非 stream/无 tool_use/无 WebSearch，并给命中响应重新生成 message id 与 usage 标记。 |
| OpenAI 兼容 | `siyuan-123` | Chat/Responses 兼容价值明确，但接口面大，容易和 Anthropic/Claude Code 主路径互相影响。 | 独立分支移植，不和当前 Claude Code 稳定性修复混合。 |
| 管理端/持久化/登录 | `gitmzc`、`BaSui01`、`coderdkai`、`Theo-jobs` | SQLite、Redis、Device Flow、自动注册、API key 池、OIDC 都是产品化能力，但凭据面和部署面风险更高。 | 排后，等核心反代链路稳定后逐项决策。 |

## 已接入功能

- Prompt cache usage 模拟：写入 Anthropic/NewAPI 兼容的 `cache_read_input_tokens`、`cache_creation_input_tokens` 和 `cache_creation.ephemeral_*` usage 字段。
- 凭据冷却与亲和：429/408/5xx 设置临时冷却，成功后清理冷却；同一 conversation 尽量绑定同一凭据，减少 cache 反复预热。
- 轻量调用记录：新增 JSONL 调用记录，记录模型、stream、凭据 ID、耗时、输入/输出 tokens、cache usage、状态、脱敏截断后的请求/响应/错误体。
- UTF-8 安全截断：用于日志和调用记录，避免按字节截断中文/多字节字符导致 panic 或乱码。
- 上游错误映射：对 context 过长、输入过长、限流、无可用凭据、400 malformed 做更明确的 HTTP 映射；详细上游错误只写本地日志，不直接透传给客户端。

## 暂缓/待决策功能

- `gitmzc` SQLite 请求流水、前端日志流、管理端重构：可观测性强，但会引入数据库 schema、管理端 API 和前端大改，建议后续单独批次。
- `hcscq` token bucket、并发队列、Redis 共享运行态、模型策略：功能强，但会显著改变调度和请求分布，存在风控与回归风险，需单独评审默认值。
- `siyuan-123` OpenAI Chat/Responses 兼容：价值明确，但接口面大，建议独立分支移植和压测。
- `coderdkai` SQLite 持久化、Device Flow、自动注册、web session 导出：产品化价值高，但凭据/登录面风险高，需你确认后再做。
- `BaSui01` API key 管理、池级路由、CSRF、历史管理：管理端改动大，和当前目标不是同一批次。
- `Theo-jobs` Redis 缓存热更新、企业 OIDC、全局/凭据代理：适合部署增强批次，不混入当前主链路。
- `Jarvisu88` response replay cache：功能正好对应“真缓存”，但该 fork 已 revert；若后续接入，需要重新设计默认值、命中标记和 tool_use 排除规则。

## 风险记录

- Top10 fork 里未发现成熟的“真实响应缓存/response replay cache”。`Jarvisu88` 曾实现 opt-in 非流式响应缓存，但随后 revert，当前只作为参考方案。
- 自动冷却、账号轮换、亲和路由会改变请求分布，可能影响上游风控，需要保守默认值。
- 大规模管理端、SQLite、Redis、OpenAI 兼容都应分批移植，避免一次性冲突和回归。
