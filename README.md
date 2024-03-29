## Анализ публикуемых новостей

### Описание

Проект предназначен для агрегации и подсчета статистик новостей из RSS рассылок. Обновление информации производится один раз в сутки.

На текущий момент реализована одна витрина данных с отображением следующей информации:
- Суррогатный ключ категории;
- Название категории;
- Общее количество новостей из всех источников по данной категории за все время;
- Количество новостей данной категории для каждого из источников за все время;
- Общее количество новостей из всех источников по данной категории за последние сутки;
- Количество новостей данной категории для каждого из источников за последние сутки;
- Среднее количество публикаций по данной категории в сутки;
- День, в который было сделано максимальное количество публикаций по данной категории;
- Количество публикаций новостей данной категории по дням недели.

Пример заполненной витрины данных

![data_mart_category.png](images%2Fdata_mart_category.png)

Блок-схема структуры проекта
![scheme.png](images%2Fscheme.png)

В качестве оркестратора используется [Airflow](https://airflow.apache.org/). Внешний вид DAG:

![dag.png](images%2Fdag.png)

ER-диаграмма (приведена для трех источников новостей)
![ERD.png](images%2FERD.png)

### Установка

Для запуска проекта необходимо установить [docker](https://docs.docker.com/engine/install/) и [docker-compose](https://docs.docker.com/compose/install/linux/).

Загрузка и настройка проекта:
``` bash
git clone https://github.com/DenisN03/RSS_News_Analysis.git
cd rss_news_analysis
mkdir ./airflow/files/
mkdir ./airflow/logs/
sudo chmod u=rwx,g=rwx,o=rwx ./airflow/files/
```

Запуск проекта:
``` bash
docker-compose up -d
```

### Настройка Airflow

Для корректной работы проекта требуется настроить соединение с БД и добавить словарь с источниками RSS данных.

Графический интерфейс Airflow доступен по адресу:
``` bash
http://localhost:8080/
```
Данные для подключения:

| Параметр | Значение |
|----------|----------|
| user     | airflow  |
| password | airflow  |


#### Настройка соединения с БД

В качестве БД используется [PostgreSQL](https://www.postgresql.org/).
Данные для подключения:

| Параметр | Значение |
|----------|----------|
| user     | admin  |
| password | admin  |
| db | news  |

В графическом интерфейсе Airflow в разделе *Connections* необходимо добавить соединение:

| Параметр | Значение |
|----------|------|
| Connection Id     | postgre_conn |
| Connection Type | Postgres |
| Host | host.docker.internal |
| Schema | news |
| Login | admin |
| Password | admin |
| Port | 5430 |

В разделе *Variables* необходимо добавить переменную:

| Параметр | Значение |
|---------|----------|
| Key | conn_id |
| Val | postgre_conn |

#### Добавление словаря с источниками RSS данных

В разделе *Variables* необходимо добавить переменную:

| Параметр    | Значение                                                                                                                |
|-------------|-------------------------------------------------------------------------------------------------------------------------|
| Key         | urls_dict                                                                                                               |
| Val         | {"lenta": "https://lenta.ru/rss/", "vedomosti":"https://www.vedomosti.ru/rss/news", "tass":"https://tass.ru/rss/v2.xml" |"
| Description | Dictionary of urls                                                                                                      |

Для добавления новых источников необходимо добавить их в указанную выше переменную. Для удаления источников необходимо убрать их из переменной. Все Task Airflow будут добавлены/удалены **автоматически**.

### Подключение к БД через GUI

Для работы с БД через графический интерфейс в проекте установлен [pgAdmin](https://www.pgadmin.org/). Он доступен по адресу:
Графический интерфейс Airflow доступен по адресу:
``` bash
http://localhost:5050/
```

Данные для подключения:

| Параметр | Значение |
|----------|----------|
| password | pgadmin  |

### Добавление файлов с RSS данными

Проект поддерживает загрузку и обработку RSS данных из фалов с жесткого диска. Для этого необходимо добавить файлы в директорию *./airflow/files/*. Файлы должны именоваться определенным образом:

| Имя файла | Дата                 | Имя источника  | Расширение  |
|----------|----------------------|----------------|--------------|
| 2022-12-20T07:27:17-tass.xml | 2022-12-20T07:27:17 | tass | xml |

Архив с данными можно загрузить из [Google Drive](https://drive.google.com/file/d/1n_tLQhmOnwfgi1dmuYE48oZLqVJniTkt/view?usp=share_link).
