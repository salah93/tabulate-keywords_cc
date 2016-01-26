# Count PubMed search results

```
pip install -r requirements.txt
py.test
crosscompute run
crosscompute serve
```

Example:

 `cat journals.txt`
 ```
 Science
 Nature
 ```

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
("Reshma Jagsi"[Author]) AND ("income"[Text Word] OR
    "poverty"[Text Word] OR "social class"[MeSH Terms] OR
        "socioeconomic factors"[MeSH Terms]) AND
        ("%s"[Date - Publication] : "%s"[Date - Publication]) AND
```
