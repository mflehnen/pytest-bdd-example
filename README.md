# Testing data pipelines with Behaviour Driven Development (BDD) 

This is an example of how to build BDD tests on data pipelines. 
Although we are using pyspark in this example project, this is not a required thing. 
We could use any other library to process the data, like pandas, polars or whatever.

## What is BDD testing

BDD testing is an Agile approach that promotes the collaboration in writing test cases in a simple language. Other 
approaches can lead to different perspectives to personas like the Product Owner, the Developer and the Tester. In BDD testing
all the perspectives are tied to a common shared language.

## What is Gherkin

[Gherkin](https://cucumber.io/docs/gherkin/reference/) is the language used by cucumber to declare tests. This language uses a minimum set of keywords
to give the structure of the tests in an executable way. 
Cucumber, on the other hand, it's a well known testing framework in java and scala world. Although we are not using cucumber in this project, the structure
of the Gherkin language is followed by many other libraries, like pytest-bdd(the one used in this project) and behave, for example.

Some of basic keywords in the Gherkin language are:

    - Scenario: Is a concrete example of the functionality of behaviour being tested.
    - Given: Represents a step used to provide the initial context of a testing scenario.
    - When: Represents a step used to provide a condition or action.
    - Then: Represents step used to provide the expected result
    - Background: Used to add an initial context that can be used by multiple scenarios
    - Feature: Used to group scenarios

Let's see some example of test definition:

![feature_example.png](docs%2Fimages%2Ffeature_example.png)

<sub>Source: The author</sub>

Looking at the example above, it is pretty clear which tests are applied and why. We have an input, an output and a context. 
There is no need to deep dive into the code to know about which scenarios are we applying in our process.

Any step like **Given**, **When**, **Then** will require a background implementation in python, but as soon as it develop it once, you can reuse it to describe any other scenarios.



## How does BDD applies to data pipelines

Nowadays, we have a lot of libraries that fits really well for testing data pipelines, but, there are some edge cases 
where the usage of BDD can shine. 
Let's imagine a use case were we have some complex reverse ETL flow with many business rules. This flow uses for example,
a datalake as an Operational Data Store (ODS) and runs a set of transformations within the datalake for feeding the resulting
data back to an application layer. That transformations executed inside the datalake contains many business rules like:

 - conditional constraints or flows, 
 - complex math calculations 
 - calculations relying on event occurrence
 
That situation can lead us to an expectation that varies for each row of our dataset. The outcome of this processing heavily depends on the data relations, specific conditions for the records; like 
the current status of a customer, or a special pricing conditions for a product and so on.

Many existing libraries  used for data testing are based on setting up our final expectation about the data. In simple terms, they are 
base on unit testing concepts: you have an input and call your transformation and check if you have an output that matches your expectation.
This is a simple and valid approach that fits very well in many cases, but it doesn't provide much context about our tests.

## The 3W's of data testing
The 3W's of data testing is a concept of this author (If you already heard about something similar, please let me know) to explain the context of our tests. 
The 3W's:
    - **What**: Provides the outcome that we are expecting from our data transformation. Generally it is a set of rows.
    - **When**: Provides context regarding events that may affect the expecting result. 
    - **Why**: Provides the reason regarding conditions that may affect our expecting result.


![3w.png](docs%2Fimages%2F3w.png)
<br><sub>Source: The author</sub>


Common challenges that we face in usual testing pipelines that are based in the expected outcome only is when someone else
does a change a function used by the data transformation pipeline. It could be a very tiny change that just changes a default parameter
of a function that defines the ordering of the resulting data, and after that, what you have is a broken pipeline. Your output is not matching the expected outcome anymore, 
and looking at your code you don't have any clue of why that happened. In these situations, a well-written BDD test scenario could be helpful to avoid struggling and time-wasting.


 

## Pros
 - Gherkin language is easy and kind of ubiquitous nowadays
 - It provides de 3W's of our expectation. We have a clear definition of what is being tested
 - The building of tests is a collaborative work
 - Code reuse. The steps implementation can be reused by different test scenarios
 - Low friction between technical and non-technical teams because the test definition and implementations are tied
 - It is also a form of documentation.

## Cons
  - It is not the best option for all the tasks (I pretend to explore the gaps in a next article). 
  - It could be expensive depending on how the tests are declared especially depending on how much data are we using in our mocks. And sometimes an effort on rewriting the Gherkin may be required in order to optimize tests execution.
  - Keeping the feature files updated could be though if you have a lot of system rules that are constantly changing.


## An implementation example 

I will provide an example project [here](https://github.com/mflehnen/pytest-bdd-example) containing the source code of the example shown previously in this article.

This project has the two main folders:

- src: contains the implementation of our data transformation functions
- tests: contains two folders 
  - features: contains the test scenarios in Gherkin language.
  - steps: contains the implementations of our tests in python using pybdd



#### Final Notes:
if you are looking for an implementation example using the behave library, you can find one [here](https://github.com/DanteLore/bdd-pyspark)

#### References:

https://cucumber.io/docs/bdd/
https://pytest-bdd.readthedocs.io/en/stable/
