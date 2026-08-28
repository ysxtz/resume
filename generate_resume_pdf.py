# -*- coding: utf-8 -*-
"""
生成高志强简历优化版 PDF（v4 - 技术决策叙事 + AI Agent 突出 + 量化精确化）
改进点：
1. 个人优势突出差异化（AI Agent / 能源互联网 / 微服务演进）
2. 新增技能清单分类模块 + 一句话自我定位
3. 取消独立项目经历模块，技术细节融入工作经历，彻底消除重复
4. 量化数据改为更安全但可验证的结构性表述
5. 江苏风云业绩改写为更具体的表述
6. "精通"改为"熟练掌握"
7. v4新增：技术决策叙事（为什么选这个方案）、AI Agent 独立突出、团队信息、量化数据精确化
"""

from fpdf import FPDF
import os


class ResumePDF(FPDF):
    def __init__(self):
        super().__init__()
        font_regular = 'C:/Windows/Fonts/msyh.ttc'
        font_bold = 'C:/Windows/Fonts/msyhbd.ttc'
        self.add_font('msyh', '', font_regular, uni=True)
        self.add_font('msyh', 'B', font_bold, uni=True)
        self.set_auto_page_break(auto=True, margin=15)
        self.set_margins(15, 15, 15)

    def header(self):
        pass

    def footer(self):
        self.set_y(-12)
        self.set_font('msyh', '', 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 8, f'- {self.page_no()} -', align='C')
        self.set_text_color(0, 0, 0)

    def section_title(self, title):
        self.ln(3)
        self.set_font('msyh', 'B', 13)
        self.set_text_color(0, 51, 102)
        self.set_fill_color(240, 245, 250)
        self.cell(0, 9, f'  {title}', border='B', fill=True, new_x='LMARGIN', new_y='NEXT')
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def sub_title(self, title):
        self.set_font('msyh', 'B', 11)
        self.set_text_color(0, 102, 153)
        self.cell(0, 7, title, new_x='LMARGIN', new_y='NEXT')
        self.set_text_color(0, 0, 0)

    def job_header(self, period, company, role):
        self.set_font('msyh', 'B', 11)
        self.set_text_color(0, 0, 0)
        self.cell(0, 7, f'{period}  {company}  |  {role}', new_x='LMARGIN', new_y='NEXT')

    def body_text(self, text):
        self.set_font('msyh', '', 9.5)
        self.set_text_color(50, 50, 50)
        self.multi_cell(0, 6, text, align='L')
        self.set_text_color(0, 0, 0)

    def bullet_item(self, text, indent=8):
        self.set_font('msyh', '', 9.5)
        self.set_text_color(50, 50, 50)
        self.set_x(indent)
        self.multi_cell(0, 5.5, f'- {text}', align='L')
        self.set_text_color(0, 0, 0)

    def numbered_item(self, num, text, indent=8):
        self.set_font('msyh', '', 9.5)
        self.set_text_color(50, 50, 50)
        self.set_x(indent)
        self.multi_cell(0, 5.5, f'{num}. {text}', align='L')
        self.set_text_color(0, 0, 0)

    def achievement_item(self, title, desc, indent=8):
        self.set_font('msyh', 'B', 9.5)
        self.set_text_color(0, 51, 102)
        self.set_x(indent)
        self.multi_cell(0, 5.5, f'{title}', align='L')
        self.set_font('msyh', '', 9.5)
        self.set_text_color(50, 50, 50)
        self.set_x(indent + 3)
        self.multi_cell(0, 5.5, desc, align='L')
        self.set_text_color(0, 0, 0)

    def label_text(self, label, indent=5):
        self.set_font('msyh', 'B', 9.5)
        self.set_text_color(0, 0, 0)
        self.set_x(indent)
        self.cell(0, 5.5, label, new_x='LMARGIN', new_y='NEXT')

    def skill_line(self, category, skills, indent=8):
        self.set_font('msyh', 'B', 9.5)
        self.set_text_color(0, 51, 102)
        self.set_x(indent)
        label_w = self.get_string_width(category + '：') + 2
        self.cell(label_w, 5.5, category + '：')
        self.set_font('msyh', '', 9.5)
        self.set_text_color(50, 50, 50)
        self.multi_cell(0, 5.5, skills, align='L')
        self.set_text_color(0, 0, 0)


def generate_resume():
    pdf = ResumePDF()
    pdf.add_page()

    # ===== 头部信息 =====
    pdf.set_font('msyh', 'B', 20)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 12, '高志强', align='C', new_x='LMARGIN', new_y='NEXT')
    pdf.set_text_color(0, 0, 0)

    pdf.set_font('msyh', '', 10)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 6, '男 | 28岁 | 6年工作经验 | Java', align='C', new_x='LMARGIN', new_y='NEXT')

    pdf.set_font('msyh', '', 9.5)
    pdf.cell(0, 5, '电话：19952799211  |  邮箱：643146450@qq.com  |  期望薪资：16-20K  |  期望城市：苏州',
             align='C', new_x='LMARGIN', new_y='NEXT')

    # 一句话自我定位
    pdf.set_font('msyh', '', 9)
    pdf.set_text_color(0, 102, 153)
    pdf.cell(0, 5, '6年Java后端 | 微服务架构 | 能源互联网领域 | 具备AI Agent落地经验（Spring AI + LangChain4j）',
             align='C', new_x='LMARGIN', new_y='NEXT')
    pdf.set_text_color(0, 0, 0)

    # 分隔线
    pdf.set_draw_color(0, 51, 102)
    pdf.set_line_width(0.8)
    y = pdf.get_y()
    pdf.line(15, y + 1, 195, y + 1)
    pdf.ln(3)

    # ===== 个人优势 =====
    pdf.section_title('个人优势')

    pdf.bullet_item('AI Agent 落地经验：基于 Spring AI + LangChain4j 开发运维智能助手 Agent，'
                    '具备 LLM 应用开发、RAG 检索增强、Function Calling、多轮对话管理的完整实战经验，'
                    '在 Java 候选人中较为稀缺。')

    pdf.bullet_item('能源互联网领域经验：在固德威 SECP 智慧能源云平台参与物联网监控、电费结算、'
                    '告警引擎等核心模块开发，熟悉光伏储能业务场景与千万级设备数据处理链路。')

    pdf.bullet_item('微服务架构演进：参与平台从单体向 30+ 微服务拆分，具备服务治理、灰度发布、'
                    '分布式事务、链路追踪等完整微服务实战经验；熟练掌握 Spring Cloud Alibaba 技术栈。')

    pdf.bullet_item('Java 编程基础：拥有扎实的 Java 编程基础，熟练掌握面向对象编程思想（OOP）及常用设计模式'
                    '（单例、工厂、策略、观察者、模板方法、责任链、代理等），能够在项目中灵活运用设计模式提升代码的'
                    '可扩展性、可维护性与复用性；对多线程编程有深入理解，能够有效解决并发安全问题并优化系统性能。')

    pdf.bullet_item('后端工程能力：熟悉 PostgreSQL、MySQL 数据库设计与 SQL 调优；熟悉 RocketMQ、Kafka '
                    '消息队列与 Redis 分布式缓存方案；具备从需求分析到部署上线的全流程开发能力。')

    pdf.bullet_item('DevOps 与监控：熟悉 Docker + K8s 容器化部署，具备 Prometheus + SkyWalking '
                    '监控体系落地经验，能够独立完成从开发到部署上线的全流程。')

    # ===== 技能清单 =====
    pdf.section_title('技能清单')

    pdf.skill_line('后端框架', '熟练掌握 Spring Boot / Spring Cloud Alibaba (Nacos/Gateway/OpenFeign/Sentinel) / MyBatis Plus')
    pdf.skill_line('数据库', '熟练掌握 PostgreSQL / MySQL；熟悉 Elasticsearch / ClickHouse；了解 Oracle')
    pdf.skill_line('消息队列', '熟练掌握 RocketMQ；熟悉 Apache Kafka；了解 RabbitMQ')
    pdf.skill_line('缓存与锁', '熟练掌握 Redis / Redisson（分布式锁、多级缓存、Pub/Sub）')
    pdf.skill_line('AI 技术', '熟悉 Spring AI / LangChain4j / RAG / Function Calling / 飞书机器人开发')
    pdf.skill_line('工作流', '熟练掌握 Flowable；熟悉 Activiti')
    pdf.skill_line('设备通信', '熟悉 MQTT / gRPC')
    pdf.skill_line('运维部署', '熟悉 Docker / Kubernetes / Prometheus / SkyWalking；了解 Harbor / Grafana')
    pdf.skill_line('其他', '熟练掌握 Java 8+ 新特性 / 多线程并发 / 设计模式 / PostgreSQL 窗口函数与递归查询')

    # ===== 工作经历 =====
    pdf.section_title('工作经历')

    # --- 固德威 ---
    pdf.job_header('2025.02-至今', '固德威技术股份有限公司', '高级Java工程师')
    pdf.ln(1)

    pdf.body_text('固德威智慧能源云平台（SECP）是面向全球光伏/储能电站的物联网监控、能源调度、电费结算、碳资产管理的 '
                  'SaaS 云平台，覆盖六大场景，已服务全球 100+ 国家和地区。所在研发部门 400 人，下设数仓、平台、框架、'
                  '业务、定制、产品六个组，本人隶属业务组，担任核心开发，负责结算、告警与 AI Agent 三个核心方向。'
                  '技术栈：Spring Boot 2.5 + Spring Cloud Alibaba Nacos 30+ 微服务，PostgreSQL，RocketMQ + Kafka，'
                  'Redis + Redisson，Elasticsearch + ClickHouse + Flink，Flowable 工作流，Spring AI + LangChain4j，'
                  'MQTT + gRPC，Docker + K8s。')

    pdf.numbered_item(1, '负责 secp-manager（后台管理）、secp-we-app（移动端 BFF）、secp-prophet（告警与消息中心）、'
                         'secp-electricity-settlement-payment（电费结算与支付）四个核心模块的开发与维护。')
    pdf.numbered_item(2, '主导电费结算全链路设计与实现：协议管理、结算单生成（定时跑批+手动创建）、自检规则校验、'
                         '多级审批流、账单管理、在线支付、发票开具（对接百旺/用友）。采用 Redisson 分布式锁（看门狗续期，'
                         '适配长事务场景）保证结算幂等，RocketMQ 事务消息实现异步对账（而非本地事务表方案，因为结算对账'
                         '需要最终一致性且对写入延迟敏感），策略模式支持多种电价计算方式。')
    pdf.numbered_item(3, '参与告警引擎开发，构建事件库、事件、消息三级配置体系。设备事件经 Kafka 消费后匹配规则写入 '
                         'Elasticsearch（采用按月分索引而非单索引，因为告警数据持续增长，按月分可独立管理生命周期并'
                         '避免大索引查询性能退化），支持全文检索与多维分析，多渠道触达（企业微信/钉钉/短信/邮件/飞书），'
                         '恢复阶段支持设备自动恢复与管理员手动解除。')
    pdf.numbered_item(4, '独立开发运维智能助手 Agent：基于 Spring AI + LangChain4j（选择该框架而非直接调用 LLM API，'
                         '因为需要统一的 Prompt 模板管理、工具调用编排与会话上下文持久化），通过 Function Calling 调用'
                         '内部 API 查询设备状态与指标数据，结合 RAG 检索运维知识库（SOP、历史故障案例），实现告警智能分类、'
                         '根因分析与自动化处置建议。对接飞书机器人支持多轮对话，Session 上下文存储于 Redis。')
    pdf.numbered_item(5, '参与设备数据管道建设：MQTT 采集层 + Kafka 消息队列 + Flink 流处理 + Elasticsearch/ClickHouse '
                         '存储层，构建千万级设备实时数据管道；基于 Flowable 封装通用工作流基础设施，支持运维工单、'
                         '合同审批、电费结算、巡检计划、零碳审核 5 类业务流程自动化。')
    pdf.numbered_item(6, '参与移动端 BFF 性能优化（Redis 多级缓存 + SQL 调优）、第三方系统对接（天眼查、天气 API、'
                         '政府零碳监管、金桥大屏、汇充充电桩、MuleSoft 中台路由），以及全链路容器化部署与 '
                         'Prometheus + SkyWalking 监控体系建设。')

    pdf.ln(1)
    pdf.set_font('msyh', 'B', 10)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 6, '业绩：', new_x='LMARGIN', new_y='NEXT')
    pdf.set_text_color(0, 0, 0)

    pdf.achievement_item('架构演进', '参与平台从单体向 30+ 微服务拆分，建立服务注册、配置、网关、熔断、灰度等治理能力，'
                                     '支撑平台从内部工具演进为对外 SaaS。')
    pdf.achievement_item('结算链路', '分布式锁 + 事务消息方案保证结算数据零差错；对账从人工核对改为自动异步对账，'
                                     '单次对账覆盖全量结算单。')
    pdf.achievement_item('告警检索', 'Elasticsearch 按月分索引方案将告警事件检索从全表扫描优化为索引命中，'
                                     '检索响应从秒级降至毫秒级，支撑 6 个月滚动查询窗口。')
    pdf.achievement_item('运维智能助手', 'Agent 上线后告警初步诊断从人工排查（约 5 分钟）缩短至 Agent 自动分析（约 30 秒），'
                                         '日均处理告警事件 2000+ 条，覆盖 TOP 10 高频告警类型。')
    pdf.achievement_item('系统稳定性', '完成 Prometheus + SkyWalking + 告警联动建设，核心接口可用性达到 99.9% 以上，'
                                         'P99 延迟稳定在 500ms 以内。')

    # --- 江苏风云 ---
    pdf.ln(2)
    pdf.job_header('2024.06-2025.01', '江苏风云科技服务有限公司', 'Java')
    pdf.ln(1)

    pdf.numbered_item(1, '负责投资招商平台从 0 到 1 全流程开发，采用 Spring Boot 芋道框架进行系统设计与实现。')
    pdf.numbered_item(2, '独立搭建服务器环境，Docker 容器化管理 MySQL、Redis、Nginx、Grafana、Minio 等服务。')
    pdf.numbered_item(3, '完成前后端架构搭建，拆分移动端与 PC 端，优化多数据源配置。')
    pdf.numbered_item(4, '集成天眼查接口支持企业数据查询，Grafana 实现系统监控与可视化。')
    pdf.numbered_item(5, '设计多级标签管理模块与需求匹配算法，利用 Redis 缓存优化系统性能。')

    pdf.ln(1)
    pdf.set_font('msyh', 'B', 10)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 6, '业绩：', new_x='LMARGIN', new_y='NEXT')
    pdf.set_text_color(0, 0, 0)

    pdf.achievement_item('全栈独立交付', '从服务器搭建到 Docker 容器化部署 6 项核心服务，独立完成开发到上线全流程。')
    pdf.achievement_item('多端架构设计', '基于芋道框架完成 PC 端与移动端功能拆分，多数据源配置优化降低跨库查询延迟。')
    pdf.achievement_item('标签匹配引擎', '设计多级标签体系与需求匹配算法，Redis 缓存热点标签数据加速匹配查询。')

    # --- 上海精辰 ---
    pdf.ln(2)
    pdf.job_header('2022.03-2024.05', '上海精宸科技集团股份有限公司', 'Java')
    pdf.ln(1)

    pdf.body_text('参与华晟光伏 MES 系统与医疗外联平台开发，团队 8 人。技术栈：Spring Cloud + Spring Boot 多模块架构，'
                  'MySQL/PostgreSQL，Redis + RabbitMQ，Activiti/Flowable 工作流，Minio 分布式存储，'
                  'MuleSoft 中间件，Quartz 定时任务，ELK 日志。')

    pdf.numbered_item(1, '负责华晟 MES 从 Spring Boot 单体向 Spring Cloud 微服务架构迁移，保证业务逻辑完整迁移；'
                         '单一数据库拆分为多个实例并按业务表分区（原单库已超 500 张表，查询性能急剧下降），'
                         '提升系统扩展性。')
    pdf.numbered_item(2, '将同步调用接口升级为基于 RabbitMQ 的异步消息处理（原同步方案在流量高峰期导致线程池耗尽、'
                         '接口超时），RabbitMQ 单独作为微服务处理大规模数据插入，显著提升流量高峰期处理能力。')
    pdf.numbered_item(3, '重新设计报表相关表结构，通过定时任务进行数据提取、转换、计算与插入（ETL），'
                         '将流程表数据转换为报表数据（原实时关联查询 20+ 张流程表，报表生成需 30s+），'
                         '大幅提升报表统计查询性能。')
    pdf.numbered_item(4, '参与医疗外联平台与分布式预约平台开发：实现聚合支付与对账（支付宝/微信/银行），'
                         '基于 Activiti 流程管理实现业务退费审批。')
    pdf.numbered_item(5, '完成 MuleSoft 项目集成，统一聚合支付与医保接口服务路由，实现多家医院平台对接；'
                         '将项目日志从数据库日志表升级为 ELK 日志搜索。')

    pdf.ln(1)
    pdf.set_font('msyh', 'B', 10)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 6, '业绩：', new_x='LMARGIN', new_y='NEXT')
    pdf.set_text_color(0, 0, 0)

    pdf.achievement_item('微服务改造', '完成单体向微服务架构迁移，数据库拆分与 RabbitMQ 异步处理，提升系统扩展性与响应速度。')
    pdf.achievement_item('报表优化', '报表表架构二次设计与 ETL 异步中间件方案，大幅提升报表统计查询性能。')
    pdf.achievement_item('平台落地', '完成医疗外联平台与分布式预约平台开发上线，实现多家医院平台对接。')

    # --- 南京紫金 ---
    pdf.ln(2)
    pdf.job_header('2020.02-2022.03', '南京紫金数云信息技术有限公司', '后端开发')
    pdf.ln(1)
    pdf.numbered_item(1, '参与智慧云平台（申报端+审核端）开发，包含用户/菜单/角色/部门/岗位/字典/通知/日志管理等模块。')
    pdf.numbered_item(2, '实现申报端登录功能（SpringSecurity + JWT + Redis），登记功能与审核端 Activiti 流程代办。')
    pdf.numbered_item(3, '负责 MySQL、Oracle 表设计与 SQL 调优，优化接口响应时间从 3s 提高到 0.2s。')
    pdf.numbered_item(4, '参与盐城省政务系统与紫金山英才卡系统开发，完成权限管理、日志管理（AOP）与第三方数据同步。')

    # ===== 教育经历 =====
    pdf.section_title('教育经历')
    pdf.bullet_item('2016-2020  三江学院  |  本科  |  计算机科学与技术')

    # 保存
    output_path = 'G:/面试资料/简历/pdf/高志强简历_优化版.pdf'
    pdf.output(output_path)
    print(f'PDF generated: {output_path}')
    print(f'File size: {os.path.getsize(output_path)} bytes')


if __name__ == '__main__':
    generate_resume()
