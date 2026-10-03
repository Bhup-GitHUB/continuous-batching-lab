package main

import "testing"

func TestParseRates(t *testing.T) {
	for _, input := range []string{"", "NaN", "Inf", "-1", "0", "2,1", "1,1", "1,garbage"} {
		if _, err := parseRates(input); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
	if rates, err := parseRates("1, 2.5,10"); err != nil || len(rates) != 3 || rates[1] != 2.5 {
		t.Fatalf("rates=%v err=%v", rates, err)
	}
}
