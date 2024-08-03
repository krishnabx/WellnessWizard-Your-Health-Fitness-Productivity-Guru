# WellnessWizard-Your-Health-Fitness-Productivity-Guru


This chatbot represents an advancement in the field of prompt engineering by querying and analysis. It leverages Langchain's language model to generate intelligent and contextually relevant prompts. It enables the users to efficiently interact with diverse data from 3 categories. Health Productivity Fitness Paving the way for improved decision-making and seamless data exploration. It harnesses the potential of Lang chain's cutting-edge language models and presents through an intuitive Streamlit user interface. The chat bot will recommend and answer queries by providing information about apps, addressing queries, and recommending relevant apps based on user input. Overall the goal is to target health enthusiasts or someone keen on productivity and empower users with diverse backgrounds to effortlessly gain insights and achieve their health and fitness aspirations.

Data Source

Web Scraping For each category, app information is scraped using the app function from the google play scraper library. The information includes details such as title, score, ratings, whether it contains ads, price, whether it's free, real installs, release date, in-app product price, and the app icon. Dataframes are created for each category using the information collected. These data frames contain selected columns of app information.
The script efficiently gathers comprehensive data on mobile applications, facilitating subsequent analysis and insights into app performance and user feedback in the specified categories.

Data Collection

Amazon S3:
The function creates an S3 client using the boto3 library, specifying AWS access key ID and secret access key for authentication. The access key ID and secret access key provided are usually kept secure. This can be particularly useful for archiving, data backup, or facilitating collaborative data analysis workflows.

Model Development

Framework - Langchain: Langchain serves as the overarching framework for the chatbot development process. It provides the structure and tools necessary to seamlessly integrate various components of the chatbot, including language models, data processing agents, and user interfaces. Langchain likely offers a set of functionalities that streamline the development workflow, making it easier for developers to work with natural language processing tasks.

Chat Model Used - ChatOpenAI: The chat model, ChatOpenAI, is a crucial component responsible for generating responses to user inputs. Leveraging advanced natural language processing techniques, ChatOpenAI interprets user queries and produces contextually relevant and coherent responses. It is trained on large datasets to understand and mimic human-like conversation, ensuring a more engaging and interactive user experience.

Agent Used for Data Processing - pandas_dataframe_agent: The pandas_dataframe_agent plays a vital role in handling and processing the underlying data. Built on the popular pandas library, this agent is likely designed to manage datasets efficiently. It handles tasks such as data cleaning, transformation, and organization, ensuring that the data fed into the chatbot is in a format that the language model can understand and utilize effectively.

LLM Model Used - GPT4: GPT4, or the fourth iteration of the Generative Pre-trained Transformer model, serves as the core language understanding and generation engine. Trained on diverse and extensive datasets, GPT4 excels in comprehending context, generating human-like text, and providing relevant responses. It forms the backbone of the chatbot's ability to understand user intent, answer queries, and engage in dynamic conversations.

In summary, the Langchain framework orchestrates the integration of components, ChatOpenAI handles user interactions, the pandas_dataframe_agent manages data processing, and GPT4 serves as the powerful language model. Together, these elements create a sophisticated and efficient chatbot capable of understanding, processing, and responding to user inputs in a natural and context-aware manner.

Streamlit Instructions:

Select for the catebory of apps from the three drop downs (Fitness, Health and Productivity)
2.Enter your query
Enter if any app summarizations needed
Look for the visualizations
Codelab Link: https://codelabs-preview.appspot.com/?file_id=11wP6_UTPNx8adUSvqu2HfzSj3Jyls0qRqGa70TaNgDM#0
