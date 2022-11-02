<?php

namespace Flowframe\Trend;

use Carbon\CarbonPeriod;
use Error;
use Flowframe\Trend\Adapters\MySqlAdapter;
use Flowframe\Trend\Adapters\PgsqlAdapter;
use Flowframe\Trend\Adapters\SqliteAdapter;
use Flowframe\Trend\Adapters\ClickHouseAdapter;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Carbon;
use Illuminate\Support\Collection;

class Trend
{
    public string $interval;

    public Carbon $start;

    public Carbon $end;

    public string $dateColumn = 'created_at';
    
    public bool $applyBetweenToQuery = true; //change

    public function __construct(public Builder $builder)
    {
    }
    
       /**
     * @param bool $applyBetween
     * @return Trend
     */
    public function setApplyBetweenToQuery(bool $applyBetween): Trend
    {
        $this->applyBetweenToQuery = $applyBetween;
        return $this;
    } //change


    public static function query(Builder $builder): self
    {
        return new static($builder);
    }

    public static function model(string $model): self
    {
        return new static($model::query());
    }

    public function between($start, $end): self
    {
        $this->start = $start;
        $this->end = $end;

        return $this;
    }

    public function interval(string $interval): self
    {
        $this->interval = $interval;

        return $this;
    }

    public function perMinute(): self
    {
        return $this->interval('minute');
    }

    public function perHour(): self
    {
        return $this->interval('hour');
    }

    public function perDay(): self
    {
        return $this->interval('day');
    }

    public function perMonth(): self
    {
        return $this->interval('month');
    }

    public function perYear(): self
    {
        return $this->interval('year');
    }

    public function dateColumn(string $column): self
    {
        $this->dateColumn = $column;

        return $this;
    }

    public function aggregate(string $column, string $aggregate): Collection
    {
        $values = $this->builder
            ->toBase()
            ->selectRaw("
                {$this->getSqlDate()} as date,
                {$aggregate}({$column}) as aggregate
            ")
             ->when($this->applyBetweenToQuery, function ($query, $role){
                $query->whereBetween($this->dateColumn, [$this->start, $this>end]);
            })  //change
            ->groupBy('date')
            ->orderBy('date')
            ->get();
        
          $values =  is_array($values) ?  collect($values) : $values; //change

        return $this->mapValuesToDates($values);
    }

    public function average(string $column): Collection
    {
        return $this->aggregate($column, 'avg');
    }

    public function min(string $column): Collection
    {
        return $this->aggregate($column, 'min');
    }

    public function max(string $column): Collection
    {
        return $this->aggregate($column, 'max');
    }

    public function sum(string $column): Collection
    {
        return $this->aggregate($column, 'sum');
    }

    public function count(string $column = '*'): Collection
    {
        return $this->aggregate($column, 'count');
    }

    public function mapValuesToDates(Collection $values): Collection
    {
        $values = $values->map(fn ($value) => new TrendValue(
            date: $value->date ?? $value['date'], //change,
            aggregate: $value->aggregate ?? $value['aggregate'] //change,
        ));

        $placeholders = $this->getDatePeriod()->map(
            fn (Carbon $date) => new TrendValue(
                date: $date->format($this->getCarbonDateFormat()),
                aggregate: 0,
            )
        );

        return $values
            ->merge($placeholders)
            ->unique('date')
            ->sort()
            ->flatten();
    }

    protected function getDatePeriod(): Collection
    {
        return collect(
            CarbonPeriod::between(
                $this->start,
                $this->end,
            )->interval("1 {$this->interval}")
        );
    }

    protected function getSqlDate(): string
    {
        $adapter = match ($this->builder->getConnection()->getDriverName()) {
            'mysql' => new MySqlAdapter(),
            'sqlite' => new SqliteAdapter(),
            'pgsql' => new PgsqlAdapter(),
             'bavix::clickhouse::custom' => new ClickHouseAdapter(), //change
            default => throw new Error('Unsupported database driver.'),
        };

        return $adapter->format($this->dateColumn, $this->interval);
    }

    protected function getCarbonDateFormat(): string
    {
        return match ($this->interval) {
            'minute' => 'Y-m-d H:i:00',
            'hour' => 'Y-m-d H:00',
            'day' => 'Y-m-d',
            'month' => 'Y-m',
            'year' => 'Y',
            default => throw new Error('Invalid interval.'),
        };
    }
}
