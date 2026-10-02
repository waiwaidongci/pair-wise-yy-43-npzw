# 溢油应急响应与任务追踪

围控、回收、岸线保护和废弃物处置任务，按证据和监测结果闭环。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8320
```

默认端口为`8320`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `POST /api/merges`：事件归并，提交`primary_item_id`、`subordinate_item_id`及双方`*_expected_version`
- `GET /api/merges/{primary_id}/{subordinate_id}`：查询已成立的归并关系
- `GET /api/items/{id}/history`：从属事件的原状态、原记录编号和归并关系
- `GET /api/audit`

允许角色：observer, response_commander, operations, viewer。估算油量、海况和未完成任务数影响响应等级；关闭前必须完成回收和岸线监测记录。

## 事件归并

同一海域两起溢油被确认为同一起时，由指挥员（response_commander）提交主事件与从属事件，
并同时给出双方当前版本；只有两边版本都未变化时归并才成立：

- 从属事件的有效记录改归主事件，主/从两侧版本各加一，从属事件保留原状态并被冻结
  （不能再登记记录或流转，状态由主事件延续）。
- 现场单号（`external_ref`）相同的记录只保留最早登记的一条（以记录编号为准），
  其余写入冲突清单，标记为`duplicate_conflict`并指向保留记录，不迁移、不计入未结事项。
- 迁入主事件的未结记录继续挡住主事件关闭。
- 同一组（主事件, 从属事件）并发提交只成立一笔；丢单的一方拿到当前归并关系
  （响应带`"replayed": true`）。写入失败整体回滚，可按原请求重试；重复提交沿用首次结果。
- 归并在主事件写`merge`审计事件、在从属事件写`merge_link`审计事件，二者共享`merge_id`，
  详情含记录去向与冲突清单，可与主事件详情和`GET /api/items/{id}/history`相互对应。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
