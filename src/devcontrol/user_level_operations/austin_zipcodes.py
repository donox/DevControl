def get_austin_zips():
    # Create dictionary of zip -> city
    zip_to_city = {
        # Travis County
        78701: "Austin", 78702: "Austin", 78703: "Austin", 78704: "Austin", 78705: "Austin",
        78708: "Austin", 78709: "Austin", 78710: "Austin", 78711: "Austin", 78712: "Austin",
        78713: "Austin", 78714: "Austin", 78715: "Austin", 78716: "Austin", 78717: "Austin",
        78718: "Austin", 78719: "Austin", 78720: "Austin", 78721: "Austin", 78722: "Austin",
        78723: "Austin", 78724: "Austin", 78725: "Austin", 78726: "Austin", 78727: "Austin",
        78728: "Austin", 78729: "Austin", 78730: "Austin", 78731: "Austin", 78732: "Austin",
        78733: "Austin", 78734: "Austin", 78735: "Austin", 78736: "Austin", 78737: "Austin",
        78738: "Austin", 78739: "Austin", 78741: "Austin", 78742: "Austin", 78744: "Austin",
        78745: "Austin", 78746: "Austin", 78747: "Austin", 78748: "Austin", 78749: "Austin",
        78750: "Austin", 78751: "Austin", 78752: "Austin", 78753: "Austin", 78754: "Austin",
        78755: "Austin", 78756: "Austin", 78757: "Austin", 78758: "Austin", 78759: "Austin",
        78760: "Austin", 78761: "Austin", 78762: "Austin", 78763: "Austin", 78764: "Austin",
        78765: "Austin", 78766: "Austin", 78767: "Austin", 78768: "Austin", 78769: "Austin",
        78660: "Pflugerville", 78691: "Pflugerville",
        78653: "Manor",
        78617: "Del Valle",
        78652: "Manchaca",

        # Williamson County
        78664: "Round Rock", 78665: "Round Rock", 78680: "Round Rock", 78681: "Round Rock",
        78682: "Round Rock", 78683: "Round Rock",
        78626: "Georgetown", 78627: "Georgetown", 78628: "Georgetown", 78633: "Georgetown",
        78613: "Cedar Park", 78630: "Cedar Park",
        78641: "Leander", 78646: "Leander",
        76574: "Taylor",
        76537: "Jarrell",
        78642: "Liberty Hill",
        78634: "Hutto",
        76527: "Florence",

        # Hays County
        78666: "San Marcos", 78667: "San Marcos",
        78640: "Kyle",
        78610: "Buda",
        78620: "Dripping Springs",
        78676: "Wimberley",
        78619: "Driftwood",

        # Bastrop County
        78602: "Bastrop",
        78621: "Elgin",
        78957: "Smithville",
        78612: "Cedar Creek",
        78662: "Red Rock",

        # Caldwell County
        78644: "Lockhart",
        78648: "Luling",
        78616: "Dale",
        78655: "Martindale",
        78656: "Maxwell"
    }

    # Create list of valid zips
    valid_zips = list(zip_to_city.keys())

    return zip_to_city, valid_zips