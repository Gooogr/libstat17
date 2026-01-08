# libstat17

## Setup

### Install dependencies
```bash
poetry install
```
### Configure environment

Run:

`python scripts/create_vk_token.py`

Save the service token to `.env`.

### Configure LLM provider

Set API keys for your LLM provider in `.env`. 
More details: https://docs.litellm.ai/docs/set_keys#environment-variables

## Run pipeline
`snakemake --cores 1`

### LLM-based steps
Also part of snakemake pipeleine, but could be run separately

## LLM Labeling
```bash
python ./scripts/label_topics.py --out-csv ./data/processed/topic_labels.csv
```
Possible group topics labels:
`book_wishes`, 
`nonbook_wishes`, 
`thank`, 
`other`

Possible item label for `nonbook_wishes`

| category             | when to use (rules)                                                                                                                                                                                                               | typical items (examples)                                                                                                                                                                                                                                                   |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `furniture`          | Use if the wish is a **large physical object** for seating, surfaces, storage, or organizing space. Includes **furniture-like storage** even if it’s “for books/toys”.                                                            | стеллаж, полка/книжная полка, шкаф, тумба, стол/парта, стул/кресло, кресло-мешок/пуф, стойка/витрина для журналов, мебель Монтессори, комод, вешалка напольная, коробки/контейнеры для хранения, органайзеры                                                               |
| `tech_equipment`     | Use if it’s a **device/electronic equipment** (powered hardware) used repeatedly. Prefer this when the core value is the **device itself**, not consumables.                                                                      | компьютер/ноутбук/планшет, принтер/МФУ (само устройство), проектор, экран, колонки/акустика, микрофон, фотоаппарат, веб-камера, сканер, ламинатор (как устройство), беспроводной пылесос (если указан как техника)                                                         |
| `supplies`           | Use for **consumables, stationery, small office items, craft materials**, and **replaceables**. Includes printer consumables (**cartridges/toner/photopaper**). If it’s something you **use up** or regularly replace → this tag. | бумага A4/цветная/картон, тетради/блокноты, папки/файлы, ручки/карандаши/фломастеры, клей/скотч, ножницы, степлер/скобы, дырокол, пленка для ламинирования, краски/кисти, пластилин, фоамиран/фетр/синельная проволока, маркеры для доски, **картриджи/тонер**, фотобумага |
| `nonbook_activities` | Use for items mainly meant for **running activities with kids/visitors**: games, puzzles, educational aids, activity inventory. If it’s primarily for **programs/engagement** (not decor and not consumable supplies) → this tag. | пазлы, настольные игры, лото/домино/шашки/шахматы, развивающие наборы, глобус, мольберт, демонстрационные материалы для занятий, наборы для опытов/творчества (как “набор”, не расходники), спортивный инвентарь для игр (мячи, обручи, скакалки)                          |
| `facility_care`      | Use for **cleaning, upkeep, safety, basic comfort** of the place. Not for festive decor. If it supports day-to-day maintenance/cleanliness/comfort → this tag.                                                                    | швабра/ведро, хозяйственные принадлежности, моющие средства (если просят), стремянка, инструменты для мелкого ремонта, мусорные пакеты, шторы/жалюзи, коврики, базовые вещи для уюта/обустройства, освещение/лампы (если как “для помещения”, не сценический свет)         |
| `event_decor`        | Use for **festive / special event vibe**: decorations, props, and event visuals. If it’s mainly for **праздники/мероприятия/оформление** → this tag.                                                                              | воздушные шары, стойка/арка для шаров, гирлянды, баннеры/растяжки, фотозона/фон, тематические украшения, конфетти, реквизит для праздника, костюмы/атрибуты для выступлений (если про “праздник/сцену”)                                                                    |
| `other`              | Use only if it **doesn’t clearly fit** any category above or is too vague/ambiguous. Prefer `other` over guessing.                                                                                                                | “нужно разное для библиотеки”, “помощь чем сможете”, редкие специфические предметы без контекста, противоречивые запросы                                                                                                                                                   |



