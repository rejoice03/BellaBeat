![Logo](https://github.com/rejoice03/BellaBeat/blob/main/bellabeat%20logo.png?raw=true)

## INTRODUCTION

Urška Sršen and Sando Mur founded Bellabeat, a high-tech company that manufactures health-focused smart products. Sršen used her background as an artist to develop beautifully designed technology that informs and inspires women around the world. Collecting data on activity, sleep, stress, and reproductive health has allowed Bellabeat to empower women with knowledge about their own health and habits. Since it was founded in 2013, Bellabeat has grown rapidly and quickly positioned itself as a tech-driven wellness company for women.

By 2016, Bellabeat had opened offices around the world and launched multiple products. Bellabeat products became available through a growing number of online retailers in addition to their own e-commerce channel on their website. The company has invested in traditional advertising media, such as radio, out-of-home billboards, print, and television, but focuses on digital marketing extensively. Bellabeat invests year-round in Google Search, maintains active Facebook and Instagram pages, and consistently engages consumers on Twitter. Additionally, Bellabeat runs video ads on Youtube and display ads on the Google Display Network to support campaigns around key marketing dates.

## BUSINESS TASK

*   To analyze FitBit Fitness Tracker Data from thirty eligible FitBit users, in order to gain some insights that could unlock new growth opportunities for the company.
*   To focus on one of Bellabeat’s products and analyze smart device data to gain insight into how customers are using their smart devices.
    For this case study, I will focus on the Bellabeat membership program. This is a subscription-based membership program that gives users 24/7 access to fully personalized guidance on nutrition, activity, sleep, health and beauty, and mindfulness-based on their lifestyle and goals.

## DATA SOURCE

*   The data is a free-to-use **Fitbit** Fitness tracker dataset made available through Mobius. It contains personal fitness tracker data from over thirty Fitbit users who have given consent to use their data.
*   There are 18 CSV files in all, but the datasets I find relevant to the product I want to focus on are our daily activity, hourly calories, hourly steps, and sleep day datasets.

## FINISHED PROJECT

**Here is my project programme code**: You can view my programme code on GitHub used for data
analysis. For Python code [**click here**](https://github.com/rejoice03/BellaBeat/blob/main/bellabeat%20Python%20Code.ipynb), for the code for SQL [**click here**](https://github.com/rejoice03/BellaBeat/blob/main/bellabeat%20SQL%20Code.sql) and for Tableau [**click here**](https://public.tableau.com/app/profile/rejoice.raju/viz/bellabeat_16777572743750/CaloriesBurntByTotalSteps).

### bellabeat Dashboard

![bellabeat Dashboard](https://github.com/rejoice03/BellaBeat/blob/main/Tableau/Dashboard%201%20\(2\).png?raw=true)

### COMPARING TOTAL STEPS AND SEDENTARY MINUTES TO CALORIES

![bellabeat Dashboard](https://github.com/rejoice03/BellaBeat/blob/main/Tableau/Calories%20Burnt%20By%20Total%20Steps.png?raw=true)
![bellabeat Dashboard](https://github.com/rejoice03/BellaBeat/blob/main/Tableau/Sedentary%20Minutes%20and%20Calories.png?raw=true)

*   This scatterplot gives us a clearer view of the correlation between TotalSteps and SedentaryMinutes to Calories.
*   The more steps taken, the more calories burnt. On the other hand, the more Sedentary minutes, the fewer calories burnt.

### SEDENTARY MINUTES VS TOTAL ACTIVE MINUTES

![bellabeat Dashboard](https://github.com/rejoice03/BellaBeat/blob/main/Tableau/Sedentary%20Minutes%20vs%20Total%20Active%20Minutes.png?raw=true)

*   There is a huge gap between SedentaryMinutes and TotalActiveMinutes.
*   It is obvious that the users spend more time sitting or lying down than they do being active. This can also say something about their occupation or lifestyle. Most likely they belong to the working class that spends countless hours behind their desk.

### TOTAL CALORIES BURNT BY DAY OF THE WEEK

![bellabeat Dashboard](https://github.com/rejoice03/BellaBeat/blob/main/Tableau/Calories%20Burned%20by%20Day%20of%20the%20Week.png?raw=true)

*   This plot shows that the most calories were burnt on Saturday and that the least calories were burnt on Monday.
*   Saturday is quite understandable being that it is a weekend and the users probably have enough time to exercise. Monday, on the other hand, is quite strange, given that is seen to be one of the busiest days at work for most people.
*   The first thing that came to mind is the fact that the users could probably be very busy behind their work desks that they barely have enough time to exercise or take a walk.

### TOTAL STEPS BY DAY OF THE WEEK

![bellabeat Dashboard](https://github.com/rejoice03/BellaBeat/blob/main/Tableau/Total%20Steps%20Taken%20by%20Day%20of%20the%20Week.png?raw=true)

*   This gives us the true picture of why Monday recorded the least Calories burnt. Here we see that the least steps were taken by the users on Monday compared to the other days of the week. Meaning they probably spent more hours sitting behind their work desks.
*   It also gives us a clue about the profile of the users in the survey. They are most likely working-class individuals.
*   Most steps were taken on Saturday and Sunday, which makes sense.

### TOTAL TIME ASLEEP BY DAY OF THE WEEK

![bellabeat Dashboard](https://github.com/rejoice03/BellaBeat/blob/main/Tableau/Total%20Time%20Asleep%20by%20Day%20of%20the%20Week.png?raw=true)

*   This tells us that the least amount of sleep was taken on Thursday and the most amount of sleep was taken on Monday. Usually, people tend to sleep longer when they are more exhausted.
*   However, we can also see that the users also slept longer hours on Saturday, Tuesday, and Sunday, which were also the days when they lost the most calories.

### THE MOST AND LEAST ACTIVE HOURS OF THE DAY

![bellabeat Dashboard](https://github.com/rejoice03/BellaBeat/blob/main/Tableau/The%20most%20and%20active%20hours%20of%20the%20day.png?raw=true)

*   Here we can see that their day starts getting really busy from 8 in the morning all the way to 7 in the evening. The least active hours of the day are between 8 p.m and 10 p.m. These are probably the best hours to reach them with targeted ads.

## RECOMMENDATION

1.  **Promotion**: The data we explored revealed that consumers are less active between 8 and 10 p.m., shortly before they retire for the night. This is the ideal moment for Bellabeat to schedule its advertising initiatives for optimal results. Google AdWords, for example, allows internet advertisers to schedule when their adverts are shown with its ad scheduling function. Such functions can allow Bellabeat to effectively reach its consumers. Scheduling adverts will also help Bellabeat save money on advertisements.
2.  **Retention**: The objective of any membership program is not limited to generating a regular stream of new members. Retention is crucial! According to an online report, eighty per cent of customers quit their subscription to a member site within the first two years of joining the site. For Bellabeat to retain its consumers, it must focus on personalized content that offers value. For instance, the data revealed a positive correlation between the number of steps walked and the number of calories burnt. The more steps a person takes each day, the more calories they burn.

*   The data also revealed that one of the issues people have is that they are hardly able to reach the minimum 10,000 steps per day recommended for healthy adults. To address this issue, Bellabeat can provide fascinating tips to assist its consumers to achieve their daily objectives. This may be published periodically on its website. Additionally, it can create engaging challenges to motivate its users to walk regularly. This will not only drive engagement but also provide enormous value to them.
*   I also noted that people spend far more time being inactive than being active. Inactivity is related to several health problems, including obesity, heart disease, stroke, etc. Bellabeat can create explainer videos and blogs to educate its users about the negative impacts of a sedentary lifestyle and how to prevent them. This is also a fantastic opportunity to promote its other products, such as the Bellabeat app, which provides users with lifestyle-related health data, and Time, which links to the Bellabeat app to offer users insights into their daily well-being.
