# Count PubMed search results


## Steps
```
# update cc.ini accordingly
$ pip install -r requirements.txt
$ crosscompute run tabulate-authors
$ crosscompute serve tabulate-authors
$ crosscompute run tabulate-journals
$ crosscompute serve tabulate-journals
```

## Example queries generated

 `cat journals.txt`
 ```
 Science
 Nature
 ```

 `cat authors.txt`
 ```
 Reshma Jagsi (temple)
 salah ahmed
 ```
add affilliations for authors in parenthesis next to the name

this allows a more precise search (sadly pubmed doesn't offer much for precision)

`cat keywords.txt`

```
 poverty
 income
```

`cat mesh.txt`

```
 social class
 socioeconomic factors
```

#### Journal tool
```
("Nature"[Journal]) AND ("income"[Text Word] OR
    "poverty"[Text Word] OR "social class"[MeSH Terms] OR
        "socioeconomic factors"[MeSH Terms]) AND
        ("%s"[Date - Publication] : "%s"[Date - Publication])
```

#### Author tool
```
("Reshma Jagsi"[Author] AND ("temple"[Affilliation])) AND ("income"[Text Word] OR
    "poverty"[Text Word] OR "social class"[MeSH Terms] OR
        "socioeconomic factors"[MeSH Terms]) AND
        ("%s"[Date - Publication] : "%s"[Date - Publication]) AND
```
