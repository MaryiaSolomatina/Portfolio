<h1 align="center"> Проекты KarpovCourses </h1>
<h3 align="left">Проекты в Airflow (создание DAG):</h3>

1. __Lesson 6__ DAG для расчета метрик каждый день за вчера

* Для каждого юзера ленты новостей расчитать число просмотров и лайков контента, а для пользователей сервиса сообщений  расчитать количество полученных и отправленных ообщений,
а также скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка в отдельном таске. Объединить две таблицы в одну
* Для этой таблицы расчитать все метрики в разрезе по полу, возрасту и ОС устройства, создав три разных таска на каждый срез
* Финальные данные со всеми метриками записать в отдельную таблицу в ClickHouse
* Каждый день таблица должна дополняться новыми данными

2. __Lesson 7__ DAG для сборки единого отчета по работе всего приложения. В отчете должна быть информация и по ленте новостей, и по сервису отправки сообщений. Отчет должен приходить ежедневно в 11:00 в telegram чат
3. __Lesson 7.2__  DAG для автоматической отправки аналитической сводки в телеграм каждое утро. Необходимо написать скрипт для сборки отчета по сервису ленты новостей. 

Отчет должен состоять из двух частей:
* текст с информацией о значениях ключевых метрик за предыдущий день
* график с значениями метрик за предыдущие 7 дней

Ключевые метрики: 
* DAU 
* Просмотры
* Лайки
* CTR
![Lesson 7.2](https://github.com/MaryiaSolomatina/Portfolio/blob/bcacd20a0066a0578291ea266ba9210c3482de66/KarpovCourses/AirFlow%20DAGs/Lesson7.2_telegram_bot.jpg)

4. __Lesson 8__ DAG для отправки оповещений при обнаружении аномалий в ключевых метриках с проверкой периодичностью каждые 15 минут (метрики - активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений). При обнаружении аномалий в telegram чат должен приходить alert (сообщение с текстом, графиком, ссылкой на оперативный дашборд)

![Lesson 8](https://github.com/MaryiaSolomatina/Portfolio/blob/bcacd20a0066a0578291ea266ba9210c3482de66/KarpovCourses/AirFlow%20DAGs/Lesson8_alerts.jpg)
