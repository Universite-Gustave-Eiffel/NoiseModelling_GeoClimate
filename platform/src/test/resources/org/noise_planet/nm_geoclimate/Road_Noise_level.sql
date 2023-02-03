DROP TABLE IF EXISTS LNIGHT_ROADS;
create table LNIGHT_ROADS (IDRECEIVER bigint NOT NULL, IDSOURCE bigint NOT NULL, HZ63 numeric(5, 2), HZ125 numeric(5, 2), HZ250 numeric(5, 2), HZ500 numeric(5, 2), HZ1000 numeric(5, 2), HZ2000 numeric(5, 2), HZ4000 numeric(5, 2), HZ8000 numeric(5, 2), LAEQ numeric(5, 2), LEQ numeric(5, 2));
DROP TABLE IF EXISTS LDEN_ROADS;
create table LDEN_ROADS (IDRECEIVER bigint NOT NULL, IDSOURCE bigint NOT NULL, HZ63 numeric(5, 2), HZ125 numeric(5, 2), HZ250 numeric(5, 2), HZ500 numeric(5, 2), HZ1000 numeric(5, 2), HZ2000 numeric(5, 2), HZ4000 numeric(5, 2), HZ8000 numeric(5, 2), LAEQ numeric(5, 2), LEQ numeric(5, 2));
INSERT INTO LNIGHT_ROADS VALUES (577176 , 1716, 8.167591597090208, 6.583495745803603, 2.496361693615057, -1.1507286755063433, -4.414555215284963, -17.33508254834409, -40.85394249717861, -99.74668809190045, 0.5165780245990519, 11.472371358350806);
INSERT INTO LNIGHT_ROADS VALUES (817251 , 1716, 13.516233076290561, 10.854845894377434, 6.150413601069147, 2.301187721083494, -1.0816433227105335, -13.977805285609733, -37.699287658954745, -97.17681252528602, 4.094655153386377, 16.158057569597744);
INSERT INTO LNIGHT_ROADS VALUES (817262 , 1716, 3.187150057198224, 1.5111217414172984, -2.6514217659651678, -6.371845651929959, -9.711020464305733, -22.933883647023734, -47.23787321732057, -108.98427417511216, -4.701874656225115, 6.419616934191329);
INSERT INTO LNIGHT_ROADS VALUES (817807 , 1716, 21.86039283938186, 23.226085188269025, 21.806344307318852, 18.68538143507739, 18.629731542892884, 9.922278170762695, -10.422547075314213, -67.04157446438343, 21.697258459427342, 28.273549126561484);
INSERT INTO LNIGHT_ROADS VALUES (817706 , 1716, 7.406654911834423, 5.812802123973398, 1.7158191902823603, -1.9390991097890833, -5.219646245563297, -18.143874129071534, -41.68761151796292, -100.62039207361504, -0.27375076875503607, 10.703098022892172);
INSERT INTO LDEN_ROADS VALUES (577176 , 1716, 17.340327469098185, 16.0984526358791, 11.954748553094788, 8.230476737803727, 5.0002438716667115, -7.858424265058517, -31.292287758441503, -90.38764681966207, 9.938128244203979, 20.813211749271446);
INSERT INTO LDEN_ROADS VALUES (3423051 , 34, 35.267426856639815, 39.92088552350588, 35.30952006220413, 33.579523463826945, 34.22482918389301, 26.937525165961787, 10.766548294327023, -33.557236245818075, 37.09339090463014, 43.426177096658414);
INSERT INTO LDEN_ROADS VALUES (3421901 , 34, 36.01978486975772, 40.666647196324206, 35.77582042340332, 33.937122859939834, 36.430450337637296, 31.617065210913253, 15.60293639580668, -26.18674551787491, 39.22063070540147, 44.44017384508277);
INSERT INTO LDEN_ROADS VALUES (3421868 , 32, 26.630778958113787, 31.232256979948694, 25.460723420734546, 23.81604552281391, 24.445436969186062, 17.451233824231146, 0.9422601626257574, -55.739662447439855, 27.408905835339112, 34.339268679724924);
INSERT INTO LDEN_ROADS VALUES (3421868 , 34, 36.474900840374474, 41.12916441412442, 36.37832169643058, 34.3554134549738, 35.289359297225204, 28.323709446789778, 13.115089219187531, -28.41615493061741, 38.1527466613377, 44.55763321778137);
INSERT INTO LDEN_ROADS VALUES (3422836 , 32, 27.61172312241103, 32.23505174072173, 26.70162352811427, 25.02563798533177, 25.980118285389736, 20.061567889788776, 4.316718439039164, -47.34882127452026, 28.99104627038046, 35.485686951385105);
INSERT INTO LDEN_ROADS VALUES (3422836 , 34, 36.65499542279855, 41.31129045268166, 36.72629662700806, 35.00712425502325, 35.76451696672737, 29.02386158049481, 14.447353917337175, -28.835263512680868, 38.673277964095085, 44.86070592155314);
INSERT INTO LNIGHT_ROADS VALUES (3422836 , 32, 17.52965483763392, 22.14489882335951, 16.606622651008536, 14.932060662271638, 15.886683868462057, 9.982643990781028, -5.740709901923559, -57.35401948719988, 18.900252746869434, 25.395885318638236);
INSERT INTO LNIGHT_ROADS VALUES (3422836 , 34, 26.57292713802144, 31.221137535319443, 26.63129574990233, 24.91354693196312, 25.671082549799692, 18.944937681487062, 4.389925576374452, -38.840461725360484, 28.582089734595094, 34.77059682341748);
alter table LNIGHT_ROADS ADD PRIMARY KEY(IDRECEIVER, IDSOURCE);
alter table LDEN_ROADS ADD PRIMARY KEY(IDRECEIVER, IDSOURCE);
