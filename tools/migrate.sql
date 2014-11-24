DROP TABLE IF EXISTS caipiao.bet365;
CREATE TABLE caipiao.bet365 (
    type VARCHAR(20) not null,
    date DATETIME not null,
    host_team VARCHAR(40) not null,
    guest_team VARCHAR(40) not null,
    result TINYINT(1) not null,            -- 1 主胜 0 平局 -1 客胜
    credit_host TINYINT(1) not null,
    credit_guest TINYINT(1) not null,
    eu_host_win_s DECIMAL(5,3),
    eu_draw_s DECIMAL(5,3),
    eu_guest_win_s DECIMAL(5,3),
    eu_host_win_e DECIMAL(5,3),
    eu_draw_e DECIMAL(5,3),
    eu_guest_win_e DECIMAL(5,3),
    score_host TINYINT(2) not null,        -- 全场主队
    score_guest TINYINT(2) not null,       -- 全场客队
    score_host_h TINYINT(2) not null,      -- 上半场主队
    score_guest_h TINYINT(2) not null,     -- 上半场客队
    as_dish_s SMALLINT(4),                 -- 盘口 = 第一个数字 * 100 + 第二个数字，例如 0.5/1 就是 0.5*100+1，1 就是 1*100
    as_guest_win_s DECIMAL(5,3),
    as_host_win_s DECIMAL(5,3),
    as_dish_e SMALLINT(4),
    as_guest_win_e DECIMAL(5,3),
    as_host_win_e DECIMAL(5,3),

    recent10_host_credit SMALLINT(4),
    recent10_guest_credit SMALLINT(4),
    recent10_host_goal SMALLINT(4),
    recent10_host_lose SMALLINT(4),
    recent10_guest_goal SMALLINT(4),
    recent10_guest_lose SMALLINT(4)
) CHARACTER SET utf8 ENGINE=MEMORY;

DROP FUNCTION IF EXISTS real_team_name;
CREATE FUNCTION real_team_name (name VARCHAR(40) CHARACTER SET utf8)
    RETURNS VARCHAR(40) CHARACTER SET utf8 DETERMINISTIC
    RETURN trim(
    if(instr(name, '[') > 0,
        substr(name, 1, instr(name, '[')-1),
        if(instr(name, '（'),
            substr(name, 0, instr(name, '（')-1),
            if(instr(name, '('),
                substr(name, 0, instr(name, '(')-1),
                name))));

INSERT INTO caipiao.bet365 SELECT
    type,
    date,
    real_team_name(host_team),
    real_team_name(eu_guest_team),
    if(eu_win_lose = '主胜', 1, if(eu_win_lose = '客胜', -1, 0)),
    if(eu_win_lose = '主胜', 3, if(eu_win_lose = '客胜', 0, 1)),
    if(eu_win_lose = '主胜', 0, if(eu_win_lose = '客胜', 3, 1)),
    eu_host_win_s,
    eu_draw_s,
    eu_guest_win_s,
    eu_host_win_e,
    eu_draw_e,
    eu_guest_win_e,
    substr(eu_score, 1, instr(eu_score, '-') - 1), -- score_host
    substr(eu_score, instr(eu_score, '-')+1, instr(eu_score, '(') - instr(eu_score, '-') - 1), -- score_guest
    substr(eu_score, instr(eu_score, '(')+1, instr(substr(eu_score, instr(eu_score, '(')), '-') - 2), -- score_host_h
    substr(eu_score, instr(eu_score, '(') + instr(substr(eu_score, instr(eu_score, '(')), '-'),
                instr(eu_score, ')') - instr(eu_score, '(') - instr(substr(eu_score, instr(eu_score, '(')), '-')),
                -- score_guest_h
    if (instr(as_dish_s, '/') = 0,
        cast(as_dish_s as decimal(3,1)) * 100,
        cast(substr(as_dish_s, 0, instr(as_dish_s, '/') - 1) as decimal(3,1)) * 100 + cast(substr(as_dish_s, instr(as_dish_s, '/') + 1) as decimal(3,1)) * 10), -- as_dish_s
    as_guest_win_s,
    as_host_win_s,
    if (instr(as_dish_e, '/') = 0,
        cast(as_dish_e as decimal(3,1)) * 100,
        cast(substr(as_dish_e, 0, instr(as_dish_e, '/') - 1) as decimal(3,1)) * 100 + cast(substr(as_dish_e, instr(as_dish_e, '/') + 1) as decimal(3,1)) * 10), -- as_dish_e
    as_guest_win_e,
    as_host_win_e,
    0, 0, 0, 0, 0, 0 -- recent
    FROM test.bet365;

USE caipiao;
CREATE INDEX idx_date ON bet365 (date);
CREATE INDEX idx_type ON bet365 (type, date);
CREATE INDEX idx_host ON bet365 (host_team, date);
CREATE INDEX idx_guest ON bet365 (guest_team, date);

-- remove duplicate entries
DROP TABLE IF EXISTS tmp;
CREATE TABLE tmp LIKE bet365;
CREATE UNIQUE INDEX idx_unique ON tmp (date, host_team, guest_team);
INSERT IGNORE INTO tmp SELECT * FROM bet365;
DROP TABLE bet365;
RENAME TABLE tmp TO bet365;
