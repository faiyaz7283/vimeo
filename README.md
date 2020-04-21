## Task

Create an application that allows users to search over the top wikipedia pages as defined by 
[this wikipedia article](https://en.wikipedia.org/wiki/Wikipedia:Multiyear_ranking_of_most_viewed_pages). 
The application should provide an API that allows users to submit a request with a free text query and receive
back an array of results containing the documents that were relevant to the input query. The documents will be limited
to the following lists which appear in the above article: 

- Top-100 list
- Countries
- Cities
- People
- Singers
- Actors
- Athletes
- Modern political leaders
- Pre-modern people
- 3rd-millennium people

### Provided

This repo provides a utility to load the data from those articles into an elasticsearch index called `wikis`. 
Running the following command will bring up an elasticsearch instance as well as a utility which will load the 
above data into that elasticsearch instance.

```
docker-compose up
```

The first time these containers are brought up, loading the data into the `wikis` index will take around five minutes.
Each run thereafter, loading the `wikis` index should happen immediately as the index will be restored from a local snapshot.

### What You'll Write

You'll add a service to the docker-compose file provided which is a search API. This API should provide a route `/search/wikis`
which can be accessed with a GET request. The route should accept the following URL parameters:

- `q` (required):  free text query
- `list` (optional): If provided, only return results from the specified list (case insensitive). 

Your API should provide JSON responses in the following format:

```
{"number_of_results": <integer>, "results": [{"title": <string title>, "link": <string link to result>}]}
```

Also provide a `/status` endpoint which will serve as a health check endpoint. Upon successful initialization of your API,
this endpoint should return the following JSON response:

```
{"status": "green"}
```

The service should be accessible over localhost on port `9250`. That is, we should be able to run the following and get back the above response.

`docker-compose up`

....after waiting for services to start up

`curl localhost:9250/status`

### How to Submit

Clone this repo and make the necessary additions. Once you've done so, zip up the directory and send us the zipfile.

### How we're evaluating

- Correctness: Does the solution conform to the specs outlined?
- Proficiency in your chosen language. We don’t care too much about which technology you used to solve this problem, 
as long as you show a good grasp of the technology you used. You might notice that the loader is written in python, but to reiterate,
this does not mean your solution must be in python.
- Readability and consistency. 
- Relevance: Your solution should surface documents which have contents and titles related to the keywords being searched for. Beyond that, don't spend too much time fine tuning
the relevance of the system.

If your solution reaches this threshold of correctness we'll use this assignment as the basis for your onsite interview.