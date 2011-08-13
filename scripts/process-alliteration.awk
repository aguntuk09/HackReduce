
BEGIN    { _ord_init() }

function _ord_init(    low, high, i, t)
{
    low = sprintf("%c", 7) # BEL is ascii 7
    if (low == "\a") {    # regular ascii
        low = 0
        high = 127
    } else if (sprintf("%c", 128 + 7) == "\a") {
        # ascii, mark parity
        low = 128
        high = 255
    } else {        # ebcdic(!)
        low = 0
        high = 255
    }

    for (i = low; i <= high; i++) {
        t = sprintf("%c", i)
        _ord_[t] = i
    }
}

function ord(str, c)
{
    # only first character is of interest
    c = substr(str, 1, 1)
    return _ord_[c]
}

function chr(c)
{
    # force c to be numeric by adding 0
    return sprintf("%c", c + 0)
}

1900 <= $2 && $2 <= 2000 {
	data[$1, $2] += $3
}

END {
	for (year = 1900; year <= 2000; ++year) {
		printf("%d ", year)
		for (letter = "A"; letter <= "Z"; letter = chr(ord(letter) + 1)) {
			printf("%d ", data[letter, year])
		}
		print ""
	}
#	for (letter = "A"; letter <= "Z"; letter = chr(ord(letter) + 1)) {
#		for (year = 1900; year <= 2000; ++year) {
#			print letter, year, data[letter, year]
#		}
#	}
}

