#Tabulate Keywords
###Get Search Count results from pubmed for specified queries:
 example:

 `cat journals.txt`
 ```
 Science,
 Nature
 ```

`cat keywords.txt`

```
 poverty,
 income
```

`cat mesh.txt`

```
 social class,
 socioeconomic factors
```

 resulting queries =
 # Journal tool
 ```
 ("Nature"[Journal]) AND ("income"[Text Word] OR
        "poverty"[Text Word] OR "social class"[MeSH Terms] OR
            "socioeconomic factors"[MeSH Terms]) AND
            ("%s"[Date - Publication] : "%s"[Date - Publication])
 ```

# Author tool
```
 ("Reshma Jagsi"[Author]) AND ("income"[Text Word] OR
        "poverty"[Text Word] OR "social class"[MeSH Terms] OR
            "socioeconomic factors"[MeSH Terms]) AND
            ("%s"[Date - Publication] : "%s"[Date - Publication]) AND
```
and the output would be their corresponding search count results found on pubmed
