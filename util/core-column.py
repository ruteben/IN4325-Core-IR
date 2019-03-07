import json
import numpy as np
import re
import io

input = "D:/git/IN4325-Core-IR/data/tables/table.json"
output = "D:/git/IN4325-Core-IR/data/tables/table-core.json"

with io.open(input, 'r') as input_file, io.open(output, 'a') as output_file:
    output = {}
    counter = 0
    for line in input_file:
        json_txt = json.loads(line)
        for json_table in json_txt:
            table = json_txt[json_table]
            num_cols = table['numCols']
            num_rows = table['numDataRows']
            output_line = ""
            if not(num_rows == 0 or num_cols == 0):
                number_of_entities = np.zeros(num_cols)
                for i in range(num_rows):
                    for j in range(num_cols):
                        if re.search("\[.+\|.+\]", table['data'][i][j]):
                            number_of_entities[j] += 1
                output[json_table] = np.argmax(number_of_entities).item()
            else:
                output[json_table] = -1
            prepend = ",\n"
            if counter == 0:
                prepend = ""
        counter += 1
        if counter % 1000 == 0:
            print("processed %d tables" % counter)
    output_file.write(json.dumps(output, indent=4))
