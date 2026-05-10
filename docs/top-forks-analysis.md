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

1. Prompt cache usage 模拟：优先参考 `easayliu`，当前集成分支已开始移植轻量适配版。
2. 限流冷却和用户亲和：优先参考 `BenedictKing`，后续小步接入 `token_manager`。
3. 调用记录：第一阶段参考 `luluxiuxiu` 的轻量 JSON 统计，第二阶段再评估 `gitmzc` 的 SQLite 请求流水。
4. OpenAI 兼容：参考 `siyuan-123`，建议单独分支/批次移植。
5. `hcscq` 的 token bucket/并发/Redis 共享态能力强，但不适合和第一批功能混合落地。

## 风险记录

- 这些 fork 里未发现完整“真实响应缓存/response replay cache”。目前看到的是 prompt cache usage 模拟，不会跳过上游调用。
- 自动冷却、账号轮换、亲和路由会改变请求分布，可能影响上游风控，需要保守默认值。
- 大规模管理端、SQLite、Redis、OpenAI 兼容都应分批移植，避免一次性冲突和回归。
